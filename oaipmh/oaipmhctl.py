import sys
import argparse
import logging
import concurrent.futures
from datetime import datetime

from oaipmh import interfaces


LOGGER = logging.getLogger(__name__)

EPILOG = """\
Copyright 2019 SciELO <scielo-dev@googlegroups.com>.
Licensed under the terms of the BSD license. Please see LICENSE in the source
code for more information.
"""

LOGGER_FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def _nestget(data, *path, default=""):
    """Obtém valores de list ou dicionários."""
    for key_or_index in path:
        try:
            data = data[key_or_index]
        except (KeyError, IndexError):
            return default
    return data


def extract_acronym(front):
    return {
        "set_spec": _nestget(front, "journal_meta", 0, "journal_publisher_id", 0),
        "set_name": _nestget(front, "journal_meta", 0, "journal_title", 0),
    }


SETS_EXTRACTORS = [extract_acronym]


def _parse_date(date):
    for fmt in ["%d %m %Y", "%d%m%Y"]:
        try:
            return datetime.strptime(date, fmt)
        except ValueError:
            continue

    raise ValueError(f"time data '{date}' does not match any known format")


class Synchronizer:
    def __init__(
        self, source: interfaces.DataConnector, reader: interfaces.TasksReader
    ):
        self.source = source
        self.reader = reader

    def _doc_front(self, id):
        """Obtém o *front-matter* do documento referenciado por `id`.

        `id` deve ser uma string de texto no formato 
        `/documents/rgTRVDFHk5GyfDgwNjKbQCJ`.
        """
        return self.source.doc_front(id)

    def _record_metadata(self, task):
        url = task["id"]
        front = self._doc_front(url)
        sets = [extractor(front) for extractor in SETS_EXTRACTORS]
        doc_id = url.rsplit("/", 1)[-1]
        pub_date = _parse_date(_nestget(front, "pub_date", 0, "text", 0))
        return {
            "url": url,
            "identifier": f"oai:scielo:{doc_id}",
            "sets": sets,
            "timestamp": datetime.utcnow(),
            "pub_date": pub_date,
        }

    def get_docs(self, tasks):
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_task = {
                executor.submit(self._record_metadata, task): task for task in tasks
            }
            for future in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                except Exception as exc:
                    LOGGER.exception('could not sync "%r": %s', task, exc)
                else:
                    print(result)

    def sync(self, since=""):
        tasks = self.reader.read(self.source.changes(since=since))
        self.get_docs(tasks.docs_to_get())


def sync(args):
    from oaipmh.adapters import kernel

    sync = Synchronizer(
        source=kernel.DataConnector(args.source), reader=kernel.TasksReader()
    )
    sync.sync()


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
