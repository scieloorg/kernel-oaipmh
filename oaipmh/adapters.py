from . import interfaces


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


class KernelChangelogStateMachine:
    def __init__(self):
        self.state = EnqueuedState()

    def on_event(self, event):
        self.state = self.state.on_event(event)

    def task(self):
        return self.state.task


class KernelTasksReader(interfaces.TasksReader):
    def read(self, changelog):
        entities, timestamp = self._process_events(changelog)
        return (
            [{"id": id, "task": state.task()} for id, state in entities.items()],
            timestamp,
        )

    def _process_events(self, changelog):
        Machine = KernelChangelogStateMachine
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
