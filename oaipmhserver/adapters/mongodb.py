import logging

import pymongo
from oaipmh import common

from .. import exceptions


LOGGER = logging.getLogger(__name__)


class MongoDB:
    """Abstrai a configuração do MongoDB de maneira que nenhum outro objeto do 
    código necessita conhecer detalhes de conexão, nome do banco de dados ou
    das coleções que armazenam cada tipo de entidade. Caso seja necessário criar
    índices, aqui é o lugar.

    :param options: (opcional) dicionário com opções que serão passadas diretamente
    na instanciação de `pymongo.MongoClient`. Veja as opções em:
    https://api.mongodb.com/python/current/api/pymongo/mongo_client.html
    """

    def __init__(
        self, uri, dbname="oaipmh", mongoclient=pymongo.MongoClient, options=None
    ):
        self._dbname = dbname
        self._uri = uri
        self._MongoClient = mongoclient
        self._client_instance = None
        self._options = options or {}

    @property
    def _client(self):
        """Posterga a instanciação de `pymongo.MongoClient` até o seu primeiro
        uso.
        """
        options = {k: v for k, v in self._options.items() if v}

        if not self._client_instance:
            self._client_instance = self._MongoClient(self._uri, **options)
            LOGGER.debug(
                "new MongoDB client created: <%r at %s>",
                self._client_instance,
                id(self._client_instance),
            )

        LOGGER.debug(
            "using MongoDB client: <%r at %s>",
            self._client_instance,
            id(self._client_instance),
        )
        return self._client_instance

    def _db(self):
        return self._client[self._dbname]

    def _collection(self, colname):
        return self._db()[colname]

    @property
    def documents(self):
        return self._collection("documents")


class Session:
    """Implementação de `interfaces.Session` para armazenamento em MongoDB.
    Trata-se de uma classe concreta e não deve ser generalizada.
    """

    def __init__(self, mongodb_client):
        self._mongodb_client = mongodb_client

    @property
    def documents(self):
        return DocumentStore(self._mongodb_client.documents)


class DocumentStore:
    """Implementação de `interfaces.ChangesDataStore` para armazenamento em 
    MongoDB.
    """

    def __init__(self, collection):
        self._collection = collection

    def add(self, doc: dict):
        try:
            self._collection.insert_one(doc)
        except pymongo.errors.DuplicateKeyError as exc:
            raise exceptions.AlreadyExists(
                'cannot add data with id "%s": %s' % (doc["_id"], exc)
            ) from None

    def upsert(self, doc: dict):
        self._collection.update({"doc_id": doc["doc_id"]}, doc, upsert=True)

    def sets(self):
        pipeline = [
            {"$group": {"_id": "$sets.set_spec", "names": {"$push": "$sets.set_name"},}}
        ]
        return sorted(
            [
                {"set_spec": r["_id"][0], "set_name": r["names"][0][0]}
                for r in self._collection.aggregate(pipeline)
            ],
            key=lambda x: x["set_spec"],
        )

    def filter(self, set=None, from_=None, until=None, offset=0, limit=10):
        return (
            OAIRecord(r)
            for r in self._collection.find({}, skip=offset, limit=limit).sort(
                "_id", pymongo.ASCENDING
            )
        )


class OAIRecord:
    def __init__(self, data):
        self.data = data

    def header(self):
        return common.Header(
            element=None,
            identifier=self._identifier(),
            datestamp=self.data["timestamp"],
            setspec=self._sets_specs(),
            deleted=False,  # TODO: armazenar o campo `deleted`?
        )

    def _identifier(self):
        return "oai:scielo.org:" + self.data["doc_id"]

    def _sets_specs(self):
        return [s["set_spec"] for s in self.data.get("sets", []) if s.get("set_spec")]

    def metadata(self):
        return common.Metadata(
            None,
            {
                "title": self._title(),
                "creator": self._creators(),
                "subject": self._subject(),
                "description": self._description(),
                "publisher": self._publisher(),
                "date": self._date(),
                "type": ["info:eu-repo/semantics/article"],
                "format": ["text/html"],
                "identifier": [],
                "source": [],
                "language": self._language(),
                "relation": self._relation(),
                "rights": ["info:eu-repo/semantics/openAccess"],
            },
        )

    def _title(self):
        return [i.get("title", "") for i in self.data.get("titles", {})]

    def _creators(self):
        result = []
        for creator in self.data.get("creators", []):
            result.append(
                ", ".join(
                    i
                    for i in [
                        creator.get("surname", "").title(),
                        creator.get("given_name", "").title(),
                    ]
                    if i
                )
            )
        return result

    def _date(self):
        pub_date = self.data.get("pub_date")
        try:
            return [pub_date.strftime("%Y-%m-%d")]
        except AttributeError:
            return []

    def _subject(self):
        return [i["kwd"].title() for i in self.data.get("keywords", []) if i.get("kwd")]

    def _description(self):
        return [
            i["description"]
            for i in self.data.get("descriptions", [])
            if i.get("description")
        ]

    def _publisher(self):
        try:
            return [self.data["publisher"]]
        except IndexError:
            return []

    def _language(self):
        try:
            return [self.data["language"]]
        except IndexError:
            return []

    def _relation(self):
        try:
            return [self.data["doi"]]
        except IndexError:
            return []
