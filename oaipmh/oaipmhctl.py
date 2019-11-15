import sys
import argparse
import logging

from oaipmh import interfaces


LOGGER = logging.getLogger(__name__)

EPILOG = """\
Copyright 2019 SciELO <scielo-dev@googlegroups.com>.
Licensed under the terms of the BSD license. Please see LICENSE in the source
code for more information.
"""

LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


class Synchronizer:
    def __init__(
        self, source: interfaces.DataConnector, reader: interfaces.TasksReader
    ):
        self.source = source
        self.reader = reader

    def sync(self, since=""):
        return self.reader.read(self.source.changes(since=since))


def sync(args):
    from pprint import pprint as pp
    from oaipmh.adapters import kernel

    sync = Synchronizer(
        source=kernel.DataConnector(args.source), reader=kernel.TasksReader()
    )
    results = sync.sync()
    pp(list(results.docs_to_get()))


def cli(argv=None):
    if argv is None:
        argv = sys.argv
    parser = argparse.ArgumentParser(
        description="SciELO OAI-PMH data provider command line utility.", epilog=EPILOG
    )
    parser.add_argument("--loglevel", default="")
    subparsers = parser.add_subparsers()

    parser_sync = subparsers.add_parser("sync", help="Sync data with a remote source.")
    parser_sync.add_argument("source", help="URI of the data source.")
    parser_sync.set_defaults(func=sync)

    args = parser.parse_args()
    # todas as mensagens serão omitidas se level > 50
    logging.basicConfig(
        level=getattr(logging, args.loglevel.upper(), 999), format=LOGGER_FMT
    )
    return args.func(args)


def main():
    try:
        sys.exit(cli())
    except KeyboardInterrupt:
        LOGGER.info("Got a Ctrl+C. Terminating the program.")
        # É convencionado no shell que o programa finalizado pelo signal de
        # código N deve retornar o código N + 128.
        sys.exit(130)
    except Exception as exc:
        LOGGER.exception(exc)
        sys.exit("An unexpected error has occurred: %s" % exc)


if __name__ == "__main__":
    main()
