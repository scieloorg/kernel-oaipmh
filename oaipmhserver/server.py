import os
from datetime import datetime

from pyramid.config import Configurator
from pyramid.view import view_config
from pyramid.response import Response
from pyramid.httpexceptions import HTTPMethodNotAllowed
from oaipmh import common, server, metadata, error

from oaipmhserver.adapters import mongodb


class OAIServer:
    def __init__(self, session, meta, formats):
        self.session = session
        self.meta = meta
        self.formats = formats

    def identify(self):
        return self.meta

    def listSets(self, cursor=0, batch_size=10):
        return [
            (s["set_spec"], s["set_name"], "") for s in self.session.documents.sets()
        ][cursor : cursor + batch_size]

    def listIdentifiers(
        self, metadataPrefix, set=None, from_=None, until=None, cursor=0, batch_size=10
    ):
        # o argumento `metadataPrefix` não é requerido pela interface mas não
        # está sendo utilizado na busca.
        return (
            r.header()
            for r in self.session.documents.filter(
                set=set, from_=from_, until=until, offset=cursor, limit=batch_size
            )
        )

    def listRecords(
        self, metadataPrefix, set=None, from_=None, until=None, cursor=0, batch_size=10
    ):
        return (
            (r.header(), r.metadata(), None)
            for r in self.session.documents.filter(
                set=set, from_=from_, until=until, offset=cursor, limit=batch_size
            )
        )

    def listMetadataFormats(self, identifier=None):
        result = [i[:3] for i in self.formats]
        if identifier:
            result = [i for i in result if i[0] == identifier]

        if not result:
            raise error.IdDoesNotExistError()

        return result

    def getRecord(self, metadataPrefix, identifier):
        doc_id = identifier.rsplit(":")[-1]
        record = self.session.documents.fetch(doc_id=doc_id)
        if not record:
            raise error.IdDoesNotExistError()

        return record.header(), record.metadata(), None


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


def parse_date(datestamp):
    fmts = ["%Y-%m-%d", "%Y-%m", "%Y"]
    for fmt in fmts:
        try:
            return datetime.strptime(datestamp, fmt)
        except ValueError:
            continue
    else:
        raise ValueError(
            "time data '%s' does not match formats '%s'" % (datestamp, fmts)
        )


def split_dsn(dsns):
    """Produz uma lista de DSNs a partir de uma string separada de DSNs separados
    por espaços ou quebras de linha. A escolha dos separadores se baseia nas
    convenções do framework Pyramid.
    """
    return [dsn.strip() for dsn in str(dsns).split() if dsn]


DEFAULT_SETTINGS = [
    (
        "oaipmh.repo.name",
        "OAIPMH_REPO_NAME",
        str,
        "SciELO - Scientific Electronic Library Online",
    ),
    (
        "oaipmh.repo.baseurl",
        "OAIPMH_REPO_BASEURL",
        str,
        "http://www.scielo.br/oai/scielo-oai.php",
    ),
    ("oaipmh.repo.protocolversion", "OAIPMH_REPO_PROTOCOLVERSION", str, "2.0"),
    (
        "oaipmh.repo.adminemails",
        "OAIPMH_REPO_ADMINEMAILS",
        lambda x: str(x).split(),
        "scielo@scielo.org",
    ),
    (
        "oaipmh.repo.earliestdatestamp",
        "OAIPMH_REPO_EARLIESTDATESTAMP",
        parse_date,
        "1998-08-01",
    ),
    ("oaipmh.repo.deletedrecord", "OAIPMH_REPO_DELETEDRECORD", str, "no"),
    ("oaipmh.repo.granularity", "OAIPMH_REPO_GRANULARITY", str, "YYYY-MM-DDThh:mm:ssZ"),
    (
        "oaipmh.repo.compression",
        "OAIPMH_REPO_COMPRESSION",
        lambda x: str(x).split(),
        "identity",
    ),
    ("oaipmh.resumptiontoken.batchsize", "OAIPMH_RESUMPTIONTOKEN_BATCHSIZE", int, 100),
    (
        "oaipmh.mongodb.dsn",
        "OAIPMH_MONGODB_DSN",
        split_dsn,
        "mongodb://localhost:27017",
    ),
]


def parse_settings(settings):
    """Analisa e retorna as configurações da app com base no arquivo .ini e env.
    As variáveis de ambiente possuem precedência em relação aos valores
    definidos no arquivo .ini.
    """
    parsed = {}
    cfg = list(DEFAULT_SETTINGS)

    for name, envkey, convert, default in cfg:
        value = os.environ.get(envkey, settings.get(name, default))
        if convert is not None:
            value = convert(value)
        parsed[name] = value

    return parsed


def server_identity(settings):
    return common.Identify(
        repositoryName=settings["oaipmh.repo.name"],
        baseURL=settings["oaipmh.repo.baseurl"],
        protocolVersion=settings["oaipmh.repo.protocolversion"],
        adminEmails=settings["oaipmh.repo.adminemails"],
        earliestDatestamp=settings["oaipmh.repo.earliestdatestamp"],
        deletedRecord=settings["oaipmh.repo.deletedrecord"],
        granularity=settings["oaipmh.repo.granularity"],
        compression=settings["oaipmh.repo.compression"],
    )


METADATA_FORMATS = [
    # Tupla com os campos: (metadataPrefix, schema, metadataNamespace, writer)
    (
        "oai_dc",
        "http://www.openarchives.org/OAI/2.0/oai_dc.xsd",
        "http://www.openarchives.org/OAI/2.0/oai_dc/",
        server.oai_dc_writer,
    ),
]


def main(global_config, **settings):
    settings.update(parse_settings(settings))
    config = Configurator(settings=settings)
    config.add_route("root", "/")
    config.scan()

    session = mongodb.Session(mongodb.MongoDB(settings["oaipmh.mongodb.dsn"]))

    metadata_registry = metadata.MetadataRegistry()

    for fmt in METADATA_FORMATS:
        metadata_registry.registerWriter(fmt[0], fmt[3])

    oaiserver = server.BatchingServer(
        OAIServer(session, meta=server_identity(settings), formats=METADATA_FORMATS),
        metadata_registry=metadata_registry,
        resumption_batch_size=settings["oaipmh.resumptiontoken.batchsize"],
    )

    config.add_request_method(lambda request: oaiserver, "oaiserver", reify=True)
    return config.make_wsgi_app()
