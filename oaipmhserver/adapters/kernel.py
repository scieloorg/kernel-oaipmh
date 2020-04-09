import os
import re
import time
import json
import logging
import functools
from datetime import datetime
from urllib.parse import urljoin

import requests

from .. import interfaces, exceptions


LOGGER = logging.getLogger(__name__)

MAX_RETRIES = int(os.environ.get("OAIPMH_MAX_RETRIES", "4"))
BACKOFF_FACTOR = float(os.environ.get("OAIPMH_BACKOFF_FACTOR", "1.2"))


class EnqueuedState:
    task = "get"

    def on_event(self, event):
        if event == "deleted":
            return DeletedState()

        return self


class DeletedState:
    task = "delete"

    def on_event(self, event):
        if event == "modified":
            return EnqueuedState()

        return self


class ChangelogStateMachine:
    def __init__(self):
        self.state = EnqueuedState()

    def on_event(self, event):
        self.state = self.state.on_event(event)

    def task(self):
        return self.state.task


class Tasks(interfaces.Tasks):
    def __init__(self, tasks, timestamp):
        self.tasks = tasks
        self.timestamp = timestamp

    def _is_document_change_task(self, task):
        """Retorna `True` caso `task` seja referente a um documento.
        """
        return bool(re.match(r"^/documents/[\w-]+$", task.get("id", "")))

    def docs(self):
        return (t for t in self.tasks if self._is_document_change_task(t))

    def docs_to_get(self):
        return (t for t in self.docs() if t.get("task") == "get")

    def docs_to_del(self):
        return (t for t in self.docs() if t.get("task") == "delete")


class TasksReader(interfaces.TasksReader):
    def read(self, changelog):
        entities, timestamp = self._process_events(changelog)
        tasks = [{"id": id, "task": state.task()} for id, state in entities.items()]
        return Tasks(tasks=tasks, timestamp=timestamp)

    def _process_events(self, changelog):
        Machine = ChangelogStateMachine
        entities = {}
        last_timestamp = None
        for entry in changelog:
            last_timestamp = entry["timestamp"]
            id = entities.setdefault(entry["id"], Machine())
            if entry.get("deleted", False):
                event = "deleted"
            else:
                event = "modified"
            id.on_event(event)

        return entities, last_timestamp


class retry_gracefully:
    """Produz decorador que torna o objeto decorado resiliente às exceções dos
    tipos informados em `exc_list`. Tenta no máximo `max_retries` vezes com
    intervalo exponencial entre as tentativas.
    """

    def __init__(
        self,
        max_retries=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        exc_list=(exceptions.RetryableError,),
    ):
        self.max_retries = int(max_retries)
        self.backoff_factor = float(backoff_factor)
        self.exc_list = tuple(exc_list)

    def _sleep(self, seconds):
        time.sleep(seconds)

    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry = 1
            while True:
                try:
                    return func(*args, **kwargs)
                except self.exc_list as exc:
                    if retry <= self.max_retries:
                        wait_seconds = self.backoff_factor ** retry
                        LOGGER.info(
                            'could not get the result for "%s" with *args "%s" '
                            'and **kwargs "%s". retrying in %s seconds '
                            "(retry #%s): %s",
                            func.__qualname__,
                            args,
                            kwargs,
                            str(wait_seconds),
                            retry,
                            exc,
                        )
                        self._sleep(wait_seconds)
                        retry += 1
                    else:
                        raise

        return wrapper


@retry_gracefully()
def fetch_data(url: str, timeout: float = 2) -> bytes:
    try:
        response = requests.get(url, timeout=timeout)
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
        raise exceptions.RetryableError(exc) from exc
    except (
        requests.exceptions.InvalidSchema,
        requests.exceptions.MissingSchema,
        requests.exceptions.InvalidURL,
    ) as exc:
        raise exceptions.NonRetryableError(exc) from exc
    else:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            if 400 <= exc.response.status_code < 500:
                raise exceptions.NonRetryableError(exc) from exc
            elif 500 <= exc.response.status_code < 600:
                raise exceptions.RetryableError(exc) from exc
            else:
                raise

    return response.content


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
    for fmt in ["%d %m %Y", "%d%m%Y", "%m %Y", "%Y"]:
        try:
            return datetime.strptime(date, fmt)
        except ValueError:
            continue

    raise ValueError(f"time data '{date}' does not match any known format")


class DataConnector(interfaces.DataConnector):
    def __init__(self, host):
        self.host = host

    def changes(self, since=""):
        """Obtém os registros de mudança ocorridos desde `since`.
        """
        last_yielded = None
        while True:
            resp_json = self._fetch_changes(since)
            has_changes = False

            for result in resp_json["results"]:
                last_yielded = result
                has_changes = True
                yield result

            if not has_changes:
                return
            else:
                since = last_yielded["timestamp"]

    def _fetch_changes(self, since):
        return json.loads(fetch_data(urljoin(self.host, f"changes?since={since}")))

    def _absolute_url(self, url):
        return urljoin(self.host, url) if not url.startswith(self.host) else url

    def _doc_front(self, url):
        """Obtém o *front-matter* do documento identificado por `url`.

        :param url: URL relativa para o documento, por exemplo 
        `/documents/rgTRVDFHk5GyfDgwNjKbQCJ`.
        """
        return json.loads(fetch_data(self._absolute_url(f"{url}/front")))

    def doc_metadata(self, url, sets_extractors=SETS_EXTRACTORS):
        """Obtém metadados do documento identificado por `url`. 

        :param url: URL relativa para o documento, por exemplo
        `/documents/rgTRVDFHk5GyfDgwNjKbQCJ`.
        """
        front = self._doc_front(url)
        sets = [extractor(front) for extractor in sets_extractors]
        doc_id = url.rsplit("/", 1)[-1]
        pub_date = _parse_date(_nestget(front, "pub_date", 0, "text", 0))
        creators = [
            {
                "surname": _nestget(c, "contrib_surname", 0),
                "given_name": _nestget(c, "contrib_given_names", 0),
            }
            for c in front.get("contrib", [])
        ]
        original_lang = _nestget(front, "article", 0, "lang", 0)

        titles = [
            {
                "lang": original_lang,
                "title": _nestget(front, "article_meta", 0, "article_title", 0),
            },
        ]

        # `descriptions` é a soma de todos os resumos disponíveis.
        descriptions = [
            {
                "lang": original_lang,
                "description": _nestget(front, "article_meta", 0, "abstract", 0),
            }
        ]
        for trans_abstract in front.get("trans_abstract", []):
            descriptions.append({
                "lang": _nestget(trans_abstract, "lang", 0),
                "description": _nestget(trans_abstract, "text", 0),
            })

        keywords = []
        for kwd_group in front.get("kwd_group", []):
            lang = _nestget(kwd_group, "lang", 0)
            for kwd in kwd_group.get("kwd", []):
                keywords.append({"lang": lang, "kwd": kwd})

        return {
            "xml_url": self._absolute_url(url),
            "doc_id": doc_id,
            "sets": sets,
            "timestamp": datetime.utcnow(),
            "pub_date": pub_date,
            "language": original_lang,
            "publisher": _nestget(front, "journal_meta", 0, "publisher_name", 0),
            "doi": _nestget(front, "article_meta", 0, "article_doi", 0),
            "creators": creators,
            "titles": titles,
            "descriptions": descriptions,
            "keywords": keywords,
            "type": _nestget(front, "article", 0, "type", 0),
            #TODO: add permissions
        }
