from typing import Iterable, Dict


class TasksReader:
    def read(self, changelog: Iterable[Dict]):
        """Get a list of tasks (get or delete) to be performed in order to
        sync data.
        """

