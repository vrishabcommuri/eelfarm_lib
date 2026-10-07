import functools
from queue import Empty
from socket import gethostname
from threading import Condition, Lock
import traceback

from requests import RequestException
# from eelbrain._utils import ScreenHandler


class PipeQueue:
    """Queue without storage capacity"""
    def __init__(self):
        self._entrance = Condition(Lock())  # needed to put an item
        self._exit = Condition(Lock())  # storag space in the pipe
        self._offer = Condition(Lock())
        self._offer.acquire()  # released when an item is available
        self._object = None
        self._exit_available = False

    def put(self, item, block=True):
        if not block:
            raise NotImplementedError
        self._entrance.acquire()
        self._exit.acquire()
        self._object = item
        self._offer.release()
        self._exit.wait()
        self._object = None
        self._exit.release()
        self._entrance.release()

    def get(self, block=True):
        self._exit_available = True
        # wait until we have an offer
        have_offer = self._offer.acquire(block)
        self._exit_available = False
        if not have_offer:
            raise Empty
        self._exit.acquire()
        out = self._object
        self._exit.notify()
        self._exit.release()
        return out

    def full(self):
        return not self._exit_available


def loop(func):
    @functools.wraps(func)
    def new(self):
        try:
            func(self)
        except Exception as exception:
            self.logger.exception("Uncaught exception in %s", func.__name__)
            self._post_info('\n\n'.join([
                f"Uncaught exception in {func.__name__} on {gethostname()}:",
                ''.join(traceback.format_tb(exception.__traceback__)),
                str(exception),
            ]))
        else:
            self.logger.debug("%s shutdown complete", func.__name__)
    return new


def format_request_error(error: RequestException):
    items = [f"{error.__class__.__name__},", str(error.args[0])]
    if error.response is not None:
        if hasattr(error.response, 'status_code'):
            items.append(f'({error.response.status_code})')
        else:
            items.append(f'({error.response})')
    return ' '.join(items)


# screen_handler = ScreenHandler()
