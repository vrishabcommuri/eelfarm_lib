# from https://web.archive.org/web/20160305151936/http://www.jejik.com/articles/2007/02/a_simple_unix_linux_daemon_in_python/
"""Generic linux daemon base class for python 3.x."""
import atexit
import os
from pathlib import Path
import signal
import sys
import time


class Daemon(object):
    """A generic daemon class.

    Usage: subclass the daemon class and override the run() method.
    """
    def __init__(self, pidfile):
        self.pidfile = Path(pidfile)

    def daemonize(self):
        """Deamonize class. UNIX double fork mechanism."""
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError as err:
            sys.stderr.write('fork #1 failed: {0}\n'.format(err))
            sys.exit(1)

        # decouple from parent environment
        os.chdir('/')
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:

                # exit from second parent
                sys.exit(0)
        except OSError as err:
            sys.stderr.write('fork #2 failed: {0}\n'.format(err))
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(os.devnull, 'r')
        so = open(os.devnull, 'a+')
        se = open(os.devnull, 'a+')

        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        atexit.register(self.delpid)

        self.pidfile.write_text(f"{os.getpid()}\n")

    def delpid(self):
        self.pidfile.unlink()

    def start(self):
        """Start the daemon."""
        if self.pidfile.exists():
            sys.stderr.write(f"pidfile {self.pidfile} exists, trying to stop previous Daemon...\n")
            self.stop()

        # Start the daemon
        self.daemonize()
        self.run()

    def stop(self):
        """Stop the daemon."""
        # Get the pid from the pidfile
        if not self.pidfile.is_file():
            sys.stderr.write(f"pidfile {self.pidfile} does not exist. Daemon not running?\n")
            return  # not an error in a restart
        pid = int(self.pidfile.read_text().strip())

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
        except OSError as err:
            e = str(err.args)
            if e.find("No such process") > 0:
                if self.pidfile.is_file():
                    self.pidfile.unlink()
            else:
                sys.stderr.write(f"Error terminating daemon: {err.args}")

    def run(self):
        """You should override this method when you subclass Daemon.

        It will be called after the process has been daemonized by start().
        """
