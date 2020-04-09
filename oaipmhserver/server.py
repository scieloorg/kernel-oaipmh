from datetime import datetime

from oaipmh import common


class OAIServer:
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


if __name__ == "__main__":
    from oaipmh import server

    s = server.Server(OAIServer())
    print(s.identify())

