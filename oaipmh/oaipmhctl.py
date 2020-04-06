import sys
import argparse
import logging
import concurrent.futures

from oaipmh import interfaces


LOGGER = logging.getLogger(__name__)

EPILOG = """\
Copyright 2019 SciELO <scielo-dev@googlegroups.com>.
Licensed under the terms of the BSD license. Please see LICENSE in the source
code for more information.
"""

LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


class PoisonPill:
    """Sinaliza para as threads que a execução da rotina deve ser abortada. 
    """

    def __init__(self):
        self.poisoned = False


class Synchronizer:
    def __init__(
        self,
        source: interfaces.DataConnector,
        dest,  # interfaces.Session
        reader: interfaces.TasksReader,
        max_concurrency: int = 4,
    ):
        self.source = source
        self.dest = dest
        self.reader = reader
        self.max_concurrency = max_concurrency

    def _record_metadata(self, task, poison_pill=None):
        if poison_pill and poison_pill.poisoned:
            return

        return self.source.doc_metadata(task["id"])

    def get_docs(self, tasks):
        ppill = PoisonPill()
        session = self.dest
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_concurrency
        ) as executor:
            try:
                future_to_task = {
                    executor.submit(self._record_metadata, task, ppill): task
                    for task in tasks
                }
                for future in concurrent.futures.as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        LOGGER.exception('could not sync "%r": %s', task, exc)
                    else:
                        session.documents.upsert(result)

            except KeyboardInterrupt:
                ppill.poisoned = True
                raise

    def sync(self, since=""):
        tasks = self.reader.read(self.source.changes(since=since))
        self.get_docs(tasks.docs_to_get())


def sync(args):
    from oaipmh.adapters import kernel, mongodb

    mongo = mongodb.MongoDB(
        [dsn.strip() for dsn in args.mongodb_dsn.split() if dsn],
        options={"replicaSet": args.replicaset},
    )

    sync = Synchronizer(
        source=kernel.DataConnector(args.source),
        dest=mongodb.Session(mongo),
        reader=kernel.TasksReader(),
        max_concurrency=args.concurrency,
    )
    sync.sync()


def cli(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        description="SciELO OAI-PMH data provider command line utility.", epilog=EPILOG
    )
    parser.add_argument("--loglevel", default="")
    subparsers = parser.add_subparsers()

    parser_sync = subparsers.add_parser("sync", help="Sync data with a remote source.")
    parser_sync.add_argument("-c", "--concurrency", type=int, default=4)
    parser_sync.add_argument("-r", "--replicaset", default="")
    parser_sync.add_argument("source", help="URI of the data source.")
    parser_sync.add_argument("mongodb_dsn", help="DSN of the data destination.")
    parser_sync.set_defaults(func=sync)

    args = parser.parse_args(argv)
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
