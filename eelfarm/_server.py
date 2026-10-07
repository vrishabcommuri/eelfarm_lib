"""The JobServer serves work jobs to workers"""
import atexit
import fnmatch
import json
import pickle
import sys
import weakref
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import timedelta
from functools import cached_property, reduce
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from itertools import chain
from logging import DEBUG, ERROR, WARNING, getLogger
from operator import attrgetter
from os.path import exists, expanduser, join
from pathlib import Path
from queue import Empty, Queue
from socket import gethostbyname, gethostname
from threading import Event, Lock, Thread
from time import time

from ._jobs import JobWithID
from ._transmit import serialize
from ._version import version
from .string import fmtxt
from .string._text import ask, enumeration

KILL_SIGNAL = None
PENDING_JOBS_FILE = join(expanduser('~'), '.eelfarm-server.pickle')
PENDING_JOBS_FILE_LOCK = Lock()


class JobServerTerminated(Exception):
    "Command is unavailable because the server has been terminated"


class DeltaTimeFormatter:

    def __init__(self):
        self.t = time()

    def __call__(self, t):
        if t is None:
            return ''
        else:
            return round((self.t - t) / 60)


class Job(JobWithID):

    def __init__(self, path, func, args, kwargs):
        JobWithID.__init__(self)
        self.path = path
        self.path_obj = Path(path)
        self.data = serialize((func, args, kwargs))
        self.bytes = sys.getsizeof(self.data)
        self.created = time()
        self.sent = None
        self.worker = None
        self.received = None
        self.picked_up = None
        self.desc = f'Job {self.id}'

    def mark_offered(self, worker):
        self.worker = worker

    def mark_failed(self):
        self.worker = None
        self.sent = None

    def mark_sent(self):
        self.sent = time()
        self.data = None

    def mark_received(self):
        self.received = time()

    def mark_picked_up(self):
        self.picked_up = time()

    def __repr__(self):
        t = DeltaTimeFormatter()
        out = f'<Job {self.id}: created {t(self.created)}'
        if self.worker:
            out += f'; offered to {self.worker}'
        if self.sent:
            out += f'; sent {t(self.sent)}'
        if self.received:
            if not self.sent:
                out += '; orphan'
            out += f'; received {t(self.received)}'
        if self.picked_up:
            out += f'; picked up {t(self.received)}'
        return out + '>'


class RequestHandler(BaseHTTPRequestHandler):
    ID = -1
    OPEN_REQUESTS = {}  # noqa: RUF012

    def __init__(self, request, client_address, server):
        self.job_server = server.job_server
        self.job_server._last_contact[client_address[0]] = time()
        self.logger = getLogger('eelfarm.server.http')
        RequestHandler.ID += 1
        self.id = self.ID
        self.OPEN_REQUESTS[self.id] = self
        BaseHTTPRequestHandler.__init__(self, request, client_address, server)

    @cached_property
    def client_repr(self):
        client_addr = self.client_address[0]
        return self.job_server._worker_info.get(client_addr, self.address_string())

    def log_message(self, format, *args, level=DEBUG):
        # superclass logs to sys.stderr
        # https://github.com/python/cpython/blob/3.7/Lib/http/server.py#L560
        # self.log_date_time_string()
        self.logger.log(level, f"%i %s {format}", self.id, self.client_repr, *args)

    def log_error(self, format, *args):
        self.log_message(format, *args, level=ERROR)

    def do_GET(self):
        if self.path in ('/next_command', '/next_job'):
            if self.path == '/next_command':
                getter = self.job_server._get_next_command
            else:
                getter = self.job_server._get_next_job
            name = self.headers.get('name')
            command = getter(self.client_address[0], name)
            self.send_response(HTTPStatus.OK)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(json.dumps(command).encode())
            self.log_message("command %s", command['command'])
        elif self.path == '/data':
            job_path = self.headers.get('job-path')
            data = self.job_server._get_job_data(job_path)
            if data:
                self.send_response(HTTPStatus.OK)
                self.send_header('Content-type', 'application/data')
                self.end_headers()
                self.write_large_data(data)
            else:
                self.send_error(HTTPStatus.NOT_FOUND)
                self.log_message("data not found: %s", job_path, level=WARNING)
        else:
            self.send_error(HTTPStatus.BAD_REQUEST)
            self.log_message("Bad GET request: %s", self.path, level=WARNING)
        del self.OPEN_REQUESTS[self.id]

    def do_POST(self):
        if self.path == '/job_received':
            job_path = self.headers.get('job-path')
            self.job_server._mark_job_sent(job_path)
            self.send_response(HTTPStatus.OK)
        elif self.path == '/job_failed':
            job_path = self.headers.get('job-path')
            self.request.settimeout(10)
            message = self.rfile.read(int(self.headers['Content-length'])).decode()
            self.log_message("Job failed: %s\n\n%s", job_path, message, level=ERROR)
            self.job_server._mark_job_failed(job_path, self.client_address[0])
            self.send_response(HTTPStatus.OK)
        elif self.path == '/info':
            self.request.settimeout(5)
            message = self.rfile.read(int(self.headers['Content-length'])).decode()
            sender = self.headers.get('name', 'Unknown sender')
            head = f"---{sender}---"
            tail = '-' * len(head)
            print(f"{head}\n{message}\n{tail}")
            self.send_response(HTTPStatus.OK)
        else:
            self.send_error(HTTPStatus.BAD_REQUEST)
            self.log_message("Bad POST request: %s", self.path, level=WARNING)
        self.end_headers()
        del self.OPEN_REQUESTS[self.id]

    def do_PUT(self):
        if self.path == '/result':
            job_id = self.headers.get('job-id')
            job_path = self.headers.get('job-path')
            data = self.rfile.read(int(self.headers['Content-length']))
            self.job_server._put_result(job_id, job_path, data)
            self.send_response(HTTPStatus.CREATED, "Created")
            self.end_headers()
        else:
            self.send_error(HTTPStatus.BAD_REQUEST)
            self.log_message("Bad PUT request: %s", self.path, level=WARNING)
        del self.OPEN_REQUESTS[self.id]

    def write_large_data(self, data, chunk_size=2_000_000_000):
        """Overcome socket.sendall() 2 GB limitation"""
        data_view = memoryview(data)
        while data_view:
            n_sent = self.wfile.write(data_view[:chunk_size])
            data_view = data_view[n_sent:]


class Server(ThreadingHTTPServer):

    def __init__(self, job_server, server_address):
        self.__job_server_ref = weakref.ref(job_server)
        ThreadingHTTPServer.__init__(self, server_address, RequestHandler)

    @property
    def job_server(self):
        return self.__job_server_ref()


class JobServer:
    """Serve jobs for eelfarm workers"""

    def __init__(self, host=None, job_queue_length=4):
        self.host = gethostname() if host is None else host
        self.ip = gethostbyname(self.host)
        self.logger = getLogger('eelfarm.server')
        # events
        self._shutdown = Event()
        self._terminate = Event()
        self.terminated = False
        self._blacklist = set()
        self._message_queue = defaultdict(list)  # messages for workers
        # server
        self._server = Server(self, (self.host, 8000))
        self._server_thread = Thread(target=self._server.serve_forever, name='Server')
        # job record
        self._jobs_by_id = {}  # by ID
        self._jobs = {}  # by path
        self._jobs_lock = Lock()  # prevent dict change during iteration
        # queues
        self._job_queue = Queue(min(job_queue_length, 1))
        self._job_requeue = Queue()
        self._failed_jobs = Queue()
        self._result_queue = Queue()
        if job_queue_length < 1:
            self.full = self._full_any_data_loaded
        else:
            self.full = self._job_queue.full
        # keep track of workers
        self._last_contact = {}
        self._worker_info = {}  # {ip: name}
        atexit.register(self.shutdown, True)

    def start(self):
        # read jobs file
        if exists(PENDING_JOBS_FILE):
            try:
                with open(PENDING_JOBS_FILE, 'rb') as fid:
                    pending = pickle.load(fid)
            except:  # noqa: E722
                self.logger.exception("Reading pending jobs")
                pending = None
            if pending:
                self.logger.info(f"{len(pending)} pending jobs")  # , checking worker states")
                self._jobs_by_id.update({job.id: job for job in pending})
                self._jobs.update({job.path: job for job in pending})
                Job._next_id = max(job.id for job in pending) + 1
            # workers = {job.worker for job in pending.values()}
            # valid = []
            # for worker in workers:
            #     pending_jobs = [key for key, job in pending.items() if job.worker == worker]
            #     success, actual_jobs = self.ping_worker(worker)
            #     if success:
            #         if len(actual_jobs) < len(pending_jobs):
            #             self.logger.warning(f"{worker} online with {len(actual_jobs)} of {len(pending_jobs)} jobs")
            #         else:
            #             self.logger.info(f"{worker} online with {len(actual_jobs)} jobs")
            #         valid.extend(actual_jobs)
            #     else:
            #         self.logger.warning(f"{worker} can't be reached ({len(pending_jobs)} jobs)")
            #         valid.extend(pending_jobs)
            # if len(pending) > len(valid):
            #     self.logger.warning(f"Dropping {len(pending) - len(valid)} jobs")
            # for key in valid:
            #     self._jobs[key] = pending[key]
        # start threads
        self._server_thread.start()
        self.logger.info("Serving at %s (%s)", self.host, self.ip)

    def _write_pending_jobs(self):
        jobs = self.pending_jobs(assigned=False)
        try:
            with PENDING_JOBS_FILE_LOCK and open(PENDING_JOBS_FILE, 'wb') as fid:
                pickle.dump(jobs, fid, pickle.HIGHEST_PROTOCOL)
        except Exception:
            self.logger.exception(f"Writing pending jobs: {jobs}")

    def pending_jobs(self, assigned=True):
        """Pending jobs

        Parameters
        ----------
        assigned : bool
            Include jobs that have been assigned to a worker but whose data has
            not been picked up (data can contain large objects!).
        """
        with self._jobs_lock:
            if assigned:
                return [job for job in self._jobs.values() if job.worker and not job.received]
            else:
                return [job for job in self._jobs.values() if job.sent and not job.received]

    def n_pending_jobs(self):
        "Sent out or Queued"
        with self._jobs_lock:
            return sum(job.sent is not None and job.received is None for job in self._jobs.values())

    def shutdown(self, block=True):
        "Schedule regular shutdown, waiting for outstanding results"
        if not self._shutdown.is_set():
            self.logger.info("Scheduling shutdown")
            self._shutdown.set()
            # empty job queues
            for queue in (self._job_queue, self._job_requeue, self._failed_jobs):
                try:
                    while True:
                        queue.get(block=False)
                except Empty:
                    pass
            self._server.shutdown()
            self._failed_jobs.put(None)

        if block and not self.terminated:
            self.logger.info("Waiting for shutdown to complete...")
            self.join()

    def join(self):
        if not self._shutdown.is_set():
            raise RuntimeError("Can not join before shutting down")
        self._server_thread.join()
        self.terminated = True
        self.logger.info("Client successfully terminated.")

    def __del__(self):
        if not self.terminated:
            self.shutdown(True)

    def _full_any_data_loaded(self):
        return any(job.data is not None for job in self._jobs.values())

    def _get_next_command(self, worker_ip, name):
        self._worker_info[worker_ip] = name
        if self._message_queue[worker_ip]:
            return self._message_queue[worker_ip].pop()
        else:
            return {'command': 'none'}

    def _get_next_job(self, worker_ip, name):
        self._worker_info[worker_ip] = name
        if worker_ip in self._blacklist:
            return {'command': 'none'}

        try:
            if not self._job_requeue.empty():
                job = self._job_requeue.get(block=False)
            else:
                job = self._job_queue.get(block=False)
        except Empty:
            return {'command': 'none'}
        else:
            job.mark_offered(worker_ip)
            return {'command': 'job', 'job-id': str(job.id), 'job-path': job.path, 'job-bytes': job.bytes, 'eelfarm-version': version}

    def _get_job_data(self, path):
        if path in self._jobs:
            return self._jobs[path].data

    def _mark_job_failed(self, path, worker):
        try:
            job = self._jobs[path]
        except KeyError:
            self.logger.warning("Received notification from %s that unknown job failed: %s", worker, path)
            return
        worker_name = self._worker_info.get(worker, worker)
        job.mark_failed()
        if job.data is None:
            self.logger.warning(f"{job.desc} failed on {worker_name}")
            self._failed_jobs.put(job)
        else:
            self.logger.warning(f"{job.desc} data not received by {worker_name}, requeueing")
            self._job_requeue.put(job)

    def _mark_job_sent(self, path):
        if path in self._jobs:
            self._jobs[path].mark_sent()
            self._write_pending_jobs()
        else:
            self.logger.warning(f"Receipt for non-existing job: {path}")

    def _put_result(self, job_id, path, data):
        try:
            job = self._jobs[path]
        except KeyError:
            self.logger.warning(f"Received result for non-existing job {job_id} at {path}")
            job = Job(path, None, None, None)
            with self._jobs_lock:
                self._jobs[job.path] = job
            self.logger.debug("Dummy job %i generated", job.id)

        if job.path_obj.exists():
            self.logger.warning("Overwriting %s", job.path)

        with job.path_obj.open('wb') as fid:
            fid.write(data)
        job.mark_received()
        self._result_queue.put(job)
        self._write_pending_jobs()

    def put(self, path, func, *args, **kwargs):
        """Register a job

        Parameters
        ----------
        path : Path
            Path at which to save the result.
        func : callable
            Function for job (e.g. :func:`eelbrain.boosting`).
        ...
            Arguments and keyword arguments to call ``func`` with.
        """
        if self._shutdown.is_set():
            raise JobServerTerminated("Client shutting down")
        path_obj = Path(path)
        path = str(path_obj)
        if path_obj.exists():
            raise ValueError(f"path={path}: file exists")
        elif not path_obj.parent.is_dir():
            raise ValueError(f"path={path}: parent is not an existing directory")
        elif path in self._jobs:
            raise ValueError(f"path={path}: job already exists")
        elif not callable(func):
            raise TypeError("Job func must be callable.")
        job = Job(path, func, args, kwargs)
        with self._jobs_lock:
            self._jobs_by_id[job.id] = job
            self._jobs[job.path] = job
        self._job_queue.put(job)

    def find_jobs(self, pattern, among=None) -> list:
        if among is None:
            among = list(self._jobs.values())
        elif isinstance(among, str):
            if among == 'pending':
                among = self.pending_jobs()
            else:
                raise ValueError(f'among={among!r}')

        if isinstance(pattern, int):
            for job in among:
                if job.id == pattern:
                    return [job.path]
            return []
        elif isinstance(pattern, str):
            if '*' in pattern:
                return fnmatch.filter((job.path for job in among), pattern)
            else:
                for job in among:
                    if pattern == job.path:
                        return [pattern]
                return []
        elif isinstance(pattern, Iterable):
            return list(chain.from_iterable(self.find_jobs(p, among) for p in pattern))
        else:
            raise TypeError(f'pattern={pattern!r}: not a valid job description')

    def get(self, block=False, timeout=None):
        """Return the ``path`` of a completed result

        See :class:`queue.Queue` for the meaning of parameters
        """
        job = self._result_queue.get(block, timeout)
        job.mark_picked_up()
        return job.path

    def get_failed(self, block=False, timeout=None):
        """Return ``path`` of jobs that failed"""
        job = self._failed_jobs.get(block, timeout)
        if job is None:
            return
        with self._jobs_lock:
            del self._jobs_by_id[job.id]
            del self._jobs[job.path]
        return job.path

    def job_exists(self, path):
        return path in self._jobs

    def info(self):
        out = fmtxt.Report(f"{self.host} ({self.ip})", date='%c')
        out.append(self.show_workers())
        out.append(fmtxt.linebreak)
        out.append(self.show_jobs(True))
        print(out)

    def show_jobs(self, finished=False, keys=False):
        keys = bool(keys)
        finished = bool(finished)
        table = fmtxt.Table('rrlrr' + 'l' * (keys + finished), caption="Times in minutes before now.")
        table.cells('Id', 'Created', 'Worker', 'Sent', 'Received')
        if finished:
            table.cell('Picked up')
        if keys:
            table.cell('Key')
        table.midrule()
        jobs = list(self._jobs.values())
        if not finished:
            jobs = (job for job in jobs if not job.picked_up)
        jobs = sorted(jobs, key=lambda job: job.id)
        t = DeltaTimeFormatter()
        for job in jobs:
            table.cell(job.id)
            table.cell(t(job.created))
            if job.worker:
                worker = self._worker_info.get(job.worker, job.worker)
            else:
                worker = ''
            table.cell(worker)
            table.cell(t(job.sent))
            table.cell(t(job.received))
            if finished:
                table.cell(t(job.picked_up))
            if keys:
                table.cell(job.path)
        return table

    def show_pending_jobs(self, sort='id', keys=False):
        keys = bool(keys)
        table = fmtxt.Table('llr' + 'l' * keys)
        table.cells('ID', 'Worker', 'Sent (min ago)')
        if keys:
            table.cell('Key')
        table.midrule()
        t = time()
        jobs = sorted(self.pending_jobs(), key=attrgetter(sort))
        for job in jobs:
            table.cell(job.id)
            table.cell(job.worker)
            if job.sent:
                table.cell(round((t - job.sent) / 60))
            elif job.received:
                table.cell('orphan')
            else:
                table.cell('not sent')
            if keys:
                table.cell(job.path)
        return table

    def show_workers(self):
        table = fmtxt.Table('llrr')
        table.cells('address', 'name', 'last contact', 'queue')
        workers = sorted(self._last_contact)
        n_queued = Counter(job.worker for job in list(self._jobs.values()) if job.worker and not job.received)
        now = time()
        for worker in workers:
            dt = now - self._last_contact[worker]
            dt_sec = timedelta(seconds=round(dt))
            name = self._worker_info.get(worker, '')
            table.cells(worker, name, str(dt_sec), n_queued[worker])
        return table

    def show_worker_stats(self):
        workers = sorted(self._last_contact)
        times = {w: [] for w in workers}
        for job in list(self._jobs.values()):
            if job.received:
                times[job.worker].append(job.received - job.sent)
        table = fmtxt.Table('llrr')
        table.cells('address', 'name', 'Jobs done', 'Average time')
        for worker in workers:
            name = self._worker_info.get(worker, '')
            n = len(times[worker])
            t = round(sum(times[worker]) / n) if n else ''
            table.cells(worker, name, n, t)
        return table

    def remove_broken_job(self, path):
        """Remove job from pending jobs (its processing failed)"""
        paths = self.find_jobs(path)
        if '*' in path:
            if not paths:
                print("No match")
                return
            print('\n'.join(paths))
            if ask(f"Delete {len(paths)} jobs?", {'yes': 'delete'}, True) != 'yes':
                return
        print(f"Deleting {len(paths)} jobs...")
        with self._jobs_lock:
            for pth in paths:
                job = self._jobs.pop(pth)
                del self._jobs_by_id[job.id]
        self._write_pending_jobs()

    def _find_workers(self, pattern):
        if not isinstance(pattern, str):
            return reduce(set.union, (self._find_workers(p) for p in pattern))
        elif set(pattern).intersection('*?'):
            out = set()
            for address, name in self._worker_info.items():
                if fnmatch.fnmatch(address, pattern) or fnmatch.fnmatch(name, pattern):
                    out.add(address)
            return out
        else:
            return {gethostbyname(pattern)}

    def remove_broken_worker(self, worker, blacklist=False):
        """Return list of jobs that were outstanding"""
        # remove jobs assigned to worker(s)
        workers = self._find_workers(worker)
        jobs = [job for job in self.pending_jobs() if job.worker in workers]
        self.remove_broken_job([job.path for job in jobs])

        if blacklist:
            self._blacklist.update(workers)
        return jobs

    def blacklist_worker(self, worker, blacklist=True):
        """Stop (or resume) sending jobs to this worker"""
        workers = self._find_workers(worker)
        if blacklist:
            self._blacklist.update(workers)
        else:
            self._blacklist.difference_update(workers)

    def shutdown_worker(self, worker):
        """Stop worker after it finishes the jobs that it has already downloaded"""
        self._send(worker, 'shutdown', desc="Scheduling shutdown of")

    def _send(self, worker, instruction, value=None, desc=None):
        workers = self._find_workers(worker)
        if not workers:
            print(f"No workers match {worker!r}")
            return
        message = {'command': instruction}
        if value is not None:
            message['value'] = value
        if desc is None:
            if value is None:
                desc = f"Sending {instruction} to"
            else:
                desc = f"Sending {instruction} {value} to"
        print(f"{desc} {enumeration(workers)}")
        for wk in workers:
            self._message_queue[wk].append(message)


