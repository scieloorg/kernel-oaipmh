from oaipmh import interfaces


class Synchronizer:
    def __init__(
        self, source: interfaces.DataConnector, reader: interfaces.TasksReader
    ):
        self.source = source
        self.reader = reader

    def sync(self, since=""):
        return self.reader.read(self.source.changes(since=since))


if __name__ == "__main__":
    from pprint import pprint as pp
    from oaipmh.adapters import kernel

    sync = Synchronizer(
        source=kernel.DataConnector("http://dsteste.scielo.br:6543"), 
        reader=kernel.TasksReader(),
    )
    results = sync.sync()
    pp(results.docs_to_get())
