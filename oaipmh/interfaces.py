from typing import Iterable, Dict, ByteString


class TasksReader:
    def read(self, changelog: Iterable[Dict]):
        """Get a list of tasks (get or delete) to be performed in order to
        sync data.
        """


class DataConnector:
    def changes(since: str = "") -> Iterable[Dict]:
        """Sequence of change-events on the data over time.
        """

    def fetch_document(doc_id: str) -> ByteString:
        """Fetch the full data for `doc_id`.
        """

    def fetch_metadata(doc_id: str) -> Dict:
        """Fetch metadata for `doc_id`.
        """
