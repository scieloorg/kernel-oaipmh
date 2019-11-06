from .. import interfaces

import requests


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
    def _is_document_change_task(self, task):
        """Returns `True` if `task` is related to a document.
        """
        return (
            task.get("id", "").startswith("/documents")
            and len(task.get("id", "").split("/")) == 3
        )

    def docs_to_get(self):
        return [
            t
            for t in self.tasks
            if t.get("task") == "get" and self._is_document_change_task(t)
        ]

    def docs_to_del(self):
        return [
            t
            for t in self.tasks
            if t.get("task") == "delete" and self._is_document_change_task(t)
        ]


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


class DataConnector(interfaces.DataConnector):
    def __init__(self, host):
        self.host = host

    def changes(self, since=""):
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
        return requests.get(f"{self.host}/changes?since={since}").json()
