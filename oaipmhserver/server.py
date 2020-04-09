from datetime import datetime

from oaipmh import common


class OAIServer:
    def __init__(self, session):
        self.session = session

    def identify(self):
        return common.Identify(
            repositoryName='Fake',
            baseURL='https://www.scielo.br/oai/',
            protocolVersion="2.0",
            adminEmails=['scielo-dev@googlegroups.com'],
            earliestDatestamp=datetime(1997, 1, 1),
            deletedRecord='transient',
            granularity='YYYY-MM-DDThh:mm:ssZ',
            compression=['identity'],
        )

    def listSets(self):
        return [(s["set_spec"], s["set_name"], "") 
                for s in self.session.documents.sets()]


if __name__ == "__main__":
    from oaipmhserver.adapters import mongodb
    from oaipmh import server

    db = mongodb.MongoDB("mongodb://localhost:27017")
    session = mongodb.Session(db)

    s = server.Server(OAIServer(session), resumption_batch_size=10)
    print(s.handleRequest({"verb": "ListSets"}))
