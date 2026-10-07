from argparse import ArgumentParser
from pathlib import Path
from socket import gethostname

from ._version import version


def start_command(args):
    from ._worker import WorkerClient, WorkerDaemon

    if args.no_serve:
        serve = args.serve
    else:
        serve = [args.host, *args.serve]

    if args.foreground:
        w = WorkerClient(args.host, serve, args.debug, screen_logger=True)
        w.logger.info("Starting Eelfarm worker, ctrl-c to terminate...")
        w.run(args.processes, args.nice)
    else:
        w = WorkerDaemon()
        w.processes = args.processes
        w.nice = args.nice
        w.log_to = args.log
        w.start(args.host, serve, args.debug)


def stop_command(args):
    from ._worker import WorkerDaemon

    w = WorkerDaemon()
    w.stop()


def main():
    hostname = gethostname()
    argparser = ArgumentParser(description="Eelfarm")
    argparser.add_argument('--version', action='version', version=version)
    subparsers = argparser.add_subparsers()

    parser = subparsers.add_parser('start', help="Start a worker.")
    parser.set_defaults(func=start_command)
    parser.add_argument('host', type=str, nargs='?', default=hostname, help=f"Host to connect to (default is local host, {hostname}).")
    parser.add_argument('-p', '--processes', type=int, default=None, help='Number of processes to use for jobs (0 to disable multiprocessing).')
    parser.add_argument('-f', '--foreground', action='store_true', help='Run in foreground, not as daemon.')
    parser.add_argument('-n', '--nice', type=int, default=None, help='Set scheduling priority (-20 is highest priority, 19 is lowest).')
    parser.add_argument('-s', '--serve', nargs='+', default=(), help='Perform work for additional hosts (specify one or more addresses).')
    parser.add_argument('--no-serve', action='store_true', help="Don't serve host.")
    parser.add_argument('--debug', nargs='?', choices=['eelfarm', 'all'], const='eelfarm', help='Set log level to debugging.')
    parser.add_argument('--log', type=Path, default=f'~/eelfarm-{hostname}.log', help='Path for log files. If a directory, the log file is named eelfarm.log. Overwrites existing file. Ignored with --foreground.')

    parser = subparsers.add_parser('stop', help="Stop a running client.")
    parser.set_defaults(func=stop_command)

    args = argparser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)
    else:
        argparser.print_help()
