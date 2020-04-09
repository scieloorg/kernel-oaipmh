from datetime import datetime

from pyramid.config import Configurator
from pyramid.view import view_config
from pyramid.response import Response
from pyramid.httpexceptions import HTTPMethodNotAllowed
from oaipmh import common, server

from oaipmhserver.adapters import mongodb


class OAIServer:
    def __init__(self, session):
        self.session = session

    def identify(self):
        return common.Identify(
            repositoryName="Fake",
            baseURL="https://www.scielo.br/oai/",
            protocolVersion="2.0",
            adminEmails=["scielo-dev@googlegroups.com"],
            earliestDatestamp=datetime(1997, 1, 1),
            deletedRecord="transient",
            granularity="YYYY-MM-DDThh:mm:ssZ",
            compression=["identity"],
        )

    def listSets(self):
        return [
            (s["set_spec"], s["set_name"], "") for s in self.session.documents.sets()
        ]


@view_config(route_name="root")
def root(request):
    if request.method == "GET":
        args = dict(request.GET)
    elif request.method == "POST":
        args = dict(request.POST)
    else:
        raise HTTPMethodNotAllowed()

    return Response(
        body=request.oaiserver.handleRequest(args),
        charset="utf-8",
        content_type="text/xml",
    )


def main(global_config, **settings):
    config = Configurator(settings=settings)
    config.add_route("root", "/")
    config.scan()

    session = mongodb.Session(mongodb.MongoDB("mongodb://localhost:27017"))
    oaiserver = server.Server(OAIServer(session), resumption_batch_size=10)

    config.add_request_method(
        lambda request: oaiserver, "oaiserver", reify=True
    )
    return config.make_wsgi_app()
