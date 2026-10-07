# Author: Christian Brodbeck <christianbrodbeck@nyu.edu>
"""Run in interpreter:

>>> from eelfarm._worker import WorkerClient
>>> w = WorkerClient('', True, screen_logger=True)
>>> w.start()
"""
from collections import Counter, defaultdict
from datetime import timedelta
from importlib.metadata import PackageNotFoundError, version as version_of
from logging import DEBUG, INFO, getLogger, Formatter
from logging.handlers import WatchedFileHandler
from queue import Empty, Queue
import re
from socket import gethostname
import sys
from threading import Thread, Event, Lock
import traceback
from time import sleep, time

import eelbrain
import psutil
import requests

from ._daemon import Daemon
from ._jobs import JobWithID, job_description
from ._transmit import serialize, deserialize
from ._utils import PipeQueue, format_request_error, screen_handler, loop
from ._version import version


COMMUNICATION_TIMEOUT = 4*60
DATA_TIMEOUT = 4*60  # 30 lead to frequently dropping downloads

KILL_SIGNAL = None
version_major = int(re.match(r'\d+', version).group(0))


class Job(JobWithID):
    func = args = kwargs = None
    data_added = started = finished = failed = result = None

    def __init__(self, job_id, host, path, data_bytes):
        JobWithID.__init__(self)
        self.host = host
        self.path = path
        self.host_job_id = job_id
        self.bytes = data_bytes
        self.created = time()
        self.header = {'job-path': self.path, 'job-id': str(self.id)}
        self.desc = f'Job {self.id} ({host} {job_id})'

    def add_data(self, func, args, kwargs):
        if not callable(func):
            raise TypeError(f"func={func}: not callable")
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.data_added = time()

    def mark_started(self):
        self.started = time()

    def mark_finished(self, result):
        self.finished = time()
        self.result = serialize(result)
        self.func = None
        self.args = None
        self.kwargs = None

    def mark_failed(self):
        self.failed = time()
        self.func = None
        self.args = None
        self.kwargs = None

    def cpu_time(self):
        if self.started:
            if self.finished:
                return self.finished - self.started
            elif self.failed:
                return self.failed - self.started
            else:
                return time() - self.started
        return 0

    @property
    def is_wip(self):
        return not self.failed and not self.finished


class WorkerDaemon(Daemon):

    def __init__(self):
        self.host = None
        self.serve = None
        self.debug = None
        self.processes = None
        self.nice = None
        self.client = None
        self.log_to = None
        Daemon.__init__(self, "/tmp/eelfarm.worker.pid")

    def start(self, host=None, serve=(), debug=False):
        if host is None:
            raise ValueError("Need to set host to start worker")
        self.host = host
        self.serve = serve
        self.debug = debug
        Daemon.start(self)

    def run(self):
        self.client = WorkerClient(self.host, self.serve, self.debug)
        # log to file
        if self.log_to is not None:
            log_to = self.log_to.expanduser()
            if log_to.is_dir():
                log_to /= 'eelfarm.log'
            # handler = TimedRotatingFileHandler(log_to, 'midnight', atTime=datetime.time(4))
            handler = WatchedFileHandler(log_to, mode='w')
            formatter = Formatter("%(levelname)3.3s  %(name)8.8s  %(asctime)s  %(message)s", "%H:%M")
            handler.setFormatter(formatter)
            handler.setLevel(DEBUG)
            logger = getLogger() if self.debug else self.client.logger
            logger.addHandler(handler)
        # start client
        self.client.logger.info("Starting daemon worker...")
        self.client.run(self.processes, self.nice)


class WorkerClient:
    """Worker client for a boosting farm

    Parameters
    ----------
    host : str
        Address of the server.
    debug : bool
        Set log level to debug.
    queue : int
        Queue length (number of jobs to download beyond the next job).
    """
    def __init__(self, host: str, serve: tuple = (), debug: str = None, queue: int = 0, screen_logger: bool = False, sleep_time: float = 10):
        self.host = host
        self.serve = [*serve]
        self.logger = getLogger('eelfarm.worker')
        if debug:
            if debug == 'all':
                logger = getLogger()
            elif debug == 'eelfarm':
                logger = self.logger
            else:
                raise ValueError(f"debug={debug!r}")
            logger.setLevel(DEBUG)
        else:
            self.logger.setLevel(INFO)
        if screen_logger:
            if screen_handler not in self.logger.handlers:
                self.logger.addHandler(screen_handler)
        self.sleep_time = float(sleep_time)
        # header for HTTP communication
        self._headers = {
            'name': gethostname(),
            'version': version,
        }
        # Queues and threads
        self.jobs = {}
        self._terminate = Event()
        self._job_in_progress_lock = Lock()
        self.download_queue = PipeQueue()
        self.in_queue = Queue(queue) if queue else PipeQueue()
        self.out_queue = Queue()
        self._last_exception = defaultdict(str)
        self.receiver = Thread(target=self.receive, name='receiver')
        self.downloader = Thread(target=self.download, name='downloader')
        self.worker = Thread(target=self.process, name='worker')
        self.sender = Thread(target=self.send, name='sender')
        self._threads_started = False
        self._joined = False

    def start(self):
        """Start threads"""
        if not self._threads_started:
            self.receiver.start()
            self.downloader.start()
            self.worker.start()
            self.sender.start()
            self._threads_started = True

    def run(self, processes=None, nice=None):
        """Block and handle stdout"""
        eelbrain.configure(processes, nice=nice)
        self.start()
        try:
            while not self._terminate.wait(60.):
                pass
        except (KeyboardInterrupt, SystemExit):
            self.terminate()
        finally:
            self.join()

    def terminate(self, block=False):
        "Terminate process after the scheduled job is done"
        if self._joined:
            return
        elif not self._terminate.is_set():
            self.logger.info("Scheduling client shutdown after finishing pending jobs...")
            self._terminate.set()
        if block:
            self.logger.debug("Waiting for shutdown to complete...")
            self.join()

    def join(self):
        if not self._terminate.is_set():
            raise RuntimeError("Can't .join() before calling .terminate()")
        self.receiver.join()
        self.downloader.join()
        self.worker.join()
        self.sender.join()
        self._joined = True
        self._post_info(f"Shutdown complete")
        self.logger.info("Shutdown complete")

    def __del__(self):
        self.terminate(True)

    def _request_command(self, address, request):
        try:
            r = requests.get(f'http://{address}:8000/next_{request}', headers=self._headers, timeout=COMMUNICATION_TIMEOUT)
            r.raise_for_status()
        except requests.Timeout:
            error = 'Timeout'
        except requests.ConnectionError:
            error = 'No connection'
        except Exception as exception:
            self.logger.exception("%s connecting %s", self.host, exception)
            error = str(exception)
        else:
            self._last_exception[address] = ''
            return r.json()

        if error == self._last_exception[address]:
            self.logger.debug(f"{address}/next_{request}: {error}")
        else:
            self.logger.info(f"{address}/next_{request}: {error}")
            self._last_exception[address] = error

    @loop
    def receive(self):
        serve_i_start = 0
        while True:
            # look for new commands
            while True:
                data = self._request_command(self.host, 'command')
                if data is None:
                    break
                elif data['command'] == 'none':
                    break
                self.logger.debug("Command: %s", data)
                if data['command'] == 'shutdown':
                    self.logger.info("Shutdown request received...")
                    self._post_info("Initiating shutdown, ceding communication")
                    self.terminate()
                    break
                elif data['command'] == 'client':
                    args = data['value'].split()
                    n = client = None
                    if len(args) == 1:
                        n, client = 1, args[0]
                    elif len(args) == 2:
                        n_str, client = args
                        if n_str.isdigit():
                            n = int(n_str)
                    if n is None:
                        self._post_info(f"Invalid argument: {data}")
                        continue
                    n_current = self.serve.count(client)
                    if n_current < n:
                        self.serve.extend([client] * (n - n_current))
                    elif n_current > n:
                        for _ in range(n_current - n):
                            self.serve.remove(client)
                    self.logger.info("Client list updated, now serving %s", Counter(self.serve))
                    self._post_info(f"Now serving:\n{self.show_hosts()}")
                elif data['command'] == 'info':
                    self._post_info(str(self.show_hosts()))
                elif data['command'] == 'version':
                    # Intalled package versions
                    packages = data.get('value', 'Python Eelfarm Eelbrain').split()
                    table = eelbrain.fmtxt.Table('ll')
                    for package in packages:
                        if package.lower() == 'python':
                            version_str = sys.version
                        else:
                            try:
                                version_str = version_of(package)
                            except PackageNotFoundError:
                                version_str = '<not found>'
                            except Exception as exception:
                                version_str = str(exception)
                        table.cells(package, version_str)
                    self._post_info(str(table))
                else:
                    self.logger.error(f"Unknown command {data}")
                    self._post_info(f"Unknown command {data}")

            # terminate
            if self._terminate.is_set():
                self.download_queue.put(KILL_SIGNAL)
                return

            if self.download_queue.full():
                sleep(self.sleep_time)
                continue

            # request job: cycle through hosts without reset
            serve = self.serve[:]
            if len(serve) == 0:
                sleep(self.sleep_time * 2)
                continue
            serve_i_start %= len(serve)
            serve = serve[serve_i_start:] + serve[:serve_i_start]
            for i, host in enumerate(serve, 1):
                data = self._request_command(host, 'job')
                if data is None:
                    continue
                elif data['command'] == 'none':
                    continue
                elif data['command'] == 'job':
                    serve_i_start += i
                    server_version = data.get('eelfarm-version', 'unknown')
                    server_major = int(re.match(r'\d+', server_version).group(0))
                    if server_major != version_major:
                        self._post_info(f'Job rejected, please update eelfarm to version {version_major}', host)
                        continue
                    try:
                        job = Job(data['job-id'], host, data['job-path'], int(data['job-bytes']))
                    except Exception as exception:
                        self.logger.exception("%s creating %s job %s", exception, host, data)
                        self._post_info(f'Exception creating job from {data}', host)
                        continue
                    break
                else:
                    self.logger.error(f"Unknown command from {host}: {data}")
                    continue
            else:  # no job received
                sleep(self.sleep_time)
                continue

            self.logger.info("%s: job received", job.desc)
            self.jobs[job.id] = job
            self.download_queue.put(job)

    @loop
    def download(self):
        while True:
            job = self.download_queue.get()
            if job is KILL_SIGNAL:
                self.in_queue.put(KILL_SIGNAL)
                return

            # Make sure that sufficient memory is available
            mem = psutil.virtual_memory()
            if job.bytes > mem.available:
                # wait until the current process is finished and try again
                with self._job_in_progress_lock:
                    mem = psutil.virtual_memory()
                    if job.bytes > mem.available:
                        message = f"too large for worker (job {job.bytes / 1024**2} MB, {mem.available / 1024**2} MB available)"
                        self.logger.info("%s: rejecting, %s", job.desc, message)
                        self._post_outcome(job, 'failed', message)
                        job.mark_failed()
                        continue

            # Download
            for i in range(5):
                if self._download(job):
                    break
                sleep(1)
            else:
                self.logger.info("%s: download failed 5 times, canceling", job.desc)
                self._post_outcome(job, 'failed', "Download failed")
                job.mark_failed()
                continue

            self.logger.info("%s: data received", job.desc)
            self.in_queue.put(job)
            # wait until process() has started
            sleep(self.sleep_time)

    def _download(self, job: Job):
        self.logger.debug("%s: starting download", job.desc)
        try:
            r = requests.get(f'http://{job.host}:8000/data', headers=job.header, timeout=DATA_TIMEOUT)
            r.raise_for_status()
            job.add_data(*deserialize(r.content))
        except requests.RequestException as exception:
            self.logger.info("%s: download %s", job.desc, format_request_error(exception))
        except Exception as exception:
            self.logger.exception("%s: downloading data %s", job.desc, exception)
            traceback_text = ''.join(traceback.format_tb(exception.__traceback__))
            self._post_info(f"{job.desc} download failed on {gethostname()}:\n{traceback_text}\n{exception}", to=job.host)
        else:
            self._post_outcome(job, 'received')
            return True

    def _post(self, what: str, to: str, message: str = None, headers: dict = None):
        thread = Thread(target=self._post_until_successful, args=(what, to, message, headers), name=f"post {what} to {to}")
        thread.start()

    def _post_until_successful(self, what: str, to: str, message: str = None, headers: dict = None):
        self.logger.debug("Start posting %s to %s", what, to)
        data = message.encode() if message else None
        while True:
            try:
                r = requests.post(f'http://{to}:8000/{what}', data, headers=headers, timeout=COMMUNICATION_TIMEOUT)
                r.raise_for_status()
                self.logger.debug("Done posting %s to %s", what, to)
                return
            except requests.RequestException as exception:
                self.logger.info("Post %s to %s failed with %s", what, to, format_request_error(exception))
            except Exception as exception:
                self.logger.exception("Post %s to %s: %s", what, to, exception)
            if self._terminate.is_set():
                break
            sleep(self.sleep_time)
        self.logger.info("Shutdown: give up posting %s to %s", what, to)

    def _post_outcome(self, job, outcome, message: str = None):
        self._post(f'job_{outcome}', job.host, message, job.header)

    def _post_info(self, message, to: str = None):
        if to is None:
            to = self.host
        self._post('info', to, message, self._headers)

    @loop
    def process(self):
        while True:
            job = self.in_queue.get()
            if job is KILL_SIGNAL:
                self.out_queue.put(KILL_SIGNAL)
                return

            self._job_in_progress_lock.acquire()
            self.logger.info("%s: starting job", job.desc)
            desc = job_description(job.func, job.args, job.kwargs)
            self.logger.debug("%s: %s", job.desc, desc)
            job.mark_started()
            try:
                result = job.func(*job.args, **job.kwargs)
            except Exception as exception:
                self.logger.exception("%s: execution %s", job.desc, exception)
                items = [
                    f"{job.desc} failed on {gethostname()}:",
                    desc,
                    ''.join(traceback.format_tb(exception.__traceback__)),
                    str(exception),
                ]
                self._post_outcome(job, 'failed', '\n\n'.join(items))
                job.mark_failed()
            else:
                job.mark_finished(result)
                self.out_queue.put(job)
            self._job_in_progress_lock.release()

    @loop
    def send(self):
        jobs_to_send = []
        kill_when_done = False
        while True:
            # only wait here if to_send is empty
            try:
                job = self.out_queue.get(block=not jobs_to_send)
                if job is KILL_SIGNAL:
                    self._post_info("All jobs finished, sending result and shutting down...")
                    kill_when_done = True
                else:
                    jobs_to_send.append(job)
            except Empty:
                pass

            # sleep at the end of the cycle if all attempts failed
            do_sleep = True
            for job in jobs_to_send[:]:
                try:
                    r = requests.put(f'http://{job.host}:8000/result', headers=job.header, data=job.result, timeout=DATA_TIMEOUT)
                    r.raise_for_status()
                except requests.RequestException as exception:
                    self.logger.info("%s: sending result %s", job.desc, format_request_error(exception))
                except Exception as exception:
                    self.logger.exception("%s: sending result %s", job.desc, exception)
                else:
                    self.logger.info("%s: result sent successfully", job.desc)
                    jobs_to_send.remove(job)
                    do_sleep = False

            if kill_when_done and not jobs_to_send:
                return
            elif do_sleep:
                sleep(self.sleep_time)

    def show_hosts(self):
        # sort jobs by host
        jobs_by_host = defaultdict(list)
        for job in self.jobs.values():
            jobs_by_host[job.host].append(job)
        # find all hosts
        hosts = set(self.serve).union(jobs_by_host)
        # find CPU times
        cpu_time = {host: 0 for host in hosts}
        for job in self.jobs.values():
            cpu_time[job.host] += job.cpu_time()
        # find n jobs
        wip = {host: sum([job.is_wip for job in jobs]) for host, jobs in jobs_by_host.items()}
        finished = {host: sum([1 if job.finished else 0 for job in jobs]) for host, jobs in jobs_by_host.items()}
        failed = {host: sum([1 if job.failed else 0 for job in jobs]) for host, jobs in jobs_by_host.items()}
        # table
        table = eelbrain.fmtxt.Table('llrrrr')
        table.cells('', 'Host', 'Done', 'WIP', 'CPU-Time', 'Failed')
        for host in sorted(hosts):
            n_serve = self.serve.count(host) or ''
            n_finished = finished.get(host) or ''
            n_wip = wip.get(host) or ''
            if cpu_time[host]:
                time = str(timedelta(seconds=round(cpu_time[host])))
            else:
                time = ''
            n_failed = failed.get(host) or ''
            table.cells(n_serve, host, n_finished, n_wip, time, n_failed)
        return table
