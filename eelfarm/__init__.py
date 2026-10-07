from ._version import version as __version__


def start_server(address=None, log='info'):
    """Start an eelfarm JobServer

    Parameters
    ----------
    address : str
        Address at which to receive incoming connections (default is
        ``socket.gethostname()``).
    log : int | str
        By default, the server logs information about jobs to the screen. Set
        to ``0`` to disable logging.

    Returns
    -------
    server : JobServer
        Running job server instance.
    """
    # Lazy imports to make command line tool faster
    import logging
    from .server import JobServer
    # from ._utils import screen_handler

    # validate log argument
    if isinstance(log, str):
        try:
            log = getattr(logging, log.upper())
        except AttributeError:
            raise ValueError(f"log={log!r}")
    elif not isinstance(log, int):
        raise TypeError(f"log={log!r}")

    server = JobServer(address)

    # configure logging
    if log:
        # server.logger.addHandler(screen_handler)
        server.logger.setLevel(log)
        logger = logging.getLogger('eelfarm.server.http')
        logger.setLevel(log)

    server.start()
    return server
