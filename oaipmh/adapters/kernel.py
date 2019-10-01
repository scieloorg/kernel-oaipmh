from .. import interfaces


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


class TasksReader(interfaces.TasksReader):
    def read(self, changelog):
        entities, timestamp = self._process_events(changelog)
        return (
            [{"id": id, "task": state.task()} for id, state in entities.items()],
            timestamp,
        )

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
