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

    def __init__(self, uri, dbname, mongoclient=pymongo.MongoClient, options=None):
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

    @property
    def variables(self):
        return self._collection("variables")

    def create_indexes(self):
        self.documents.create_index(
            [("timestamp", pymongo.ASCENDING)], unique=False, background=True
        )


class Session:
    """Implementação de `interfaces.Session` para armazenamento em MongoDB.
    Trata-se de uma classe concreta e não deve ser generalizada.
    """

    def __init__(self, mongodb_client, context=None):
        """
        param context: dicionário usado para injetar dependências.
        """
        self._mongodb_client = mongodb_client
        self._context = context or {}

    @property
    def documents(self):
        return DocumentStore(self._mongodb_client.documents, context=self._context)

    @property
    def variables(self):
        return VariableStore(self._mongodb_client.variables)


def _parse_date(date):
    for fmt in ["%Y-%m-%dT%H:%M:%SZ"]:
        try:
            return datetime.strptime(date, fmt)
        except ValueError:
            continue

    raise ValueError(f"time data '{date}' does not match any known format")


class DocumentStore:
    """Implementação de `interfaces.ChangesDataStore` para armazenamento em 
    MongoDB.
    """

    def __init__(self, collection, context):
        self._collection = collection
        self._context = context

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
        query_params = {}
        if set:
            query_params["sets.set_spec"] = set
        if from_:
            query_params["timestamp"] = {"$gte": from_}
        if until:
            query_params["timestamp"] = {"$lte": until}

        return (
            OAIRecord(r, context=self._context)
            for r in self._collection.find(query_params, skip=offset, limit=limit).sort(
                "timestamp", pymongo.ASCENDING
            )
        )

    def fetch(self, doc_id):
        raw_record = self._collection.find_one({"doc_id": doc_id})
        if raw_record:
            return OAIRecord(raw_record, context=self._context)
        else:
            return None

    def earliest_datestamp(self):
        cursor = self._collection.find(
            {},
            sort=[("timestamp", pymongo.ASCENDING)],
            projection={"timestamp": True, "_id": False},
        ).limit(1)

        if cursor.count() < 1:
            return None

        raw_record = next(cursor)
        return raw_record.get("timestamp")


class VariableStore:
    """Armazena variáveis da aplicação.
    """

    def __init__(self, collection):
        self._collection = collection

    def upsert(self, name, value):
        self._collection.update(
            {"_id": name}, {"_id": name, "value": value}, upsert=True
        )

    def fetch(self, name, default=""):
        raw_record = self._collection.find_one({"_id": name}) or {}
        return raw_record.get("value", default)


# Mapeamento definido pela equipe do OpenAIRE
ARTICLETYPE_TO_VOCABULARY_MAP = {
    "research-article": "info:eu-repo/semantics/article",
    "article-commentary": "info:eu-repo/semantics/other",
    "book-review": "info:eu-repo/semantics/review",
    "brief-report": "info:eu-repo/semantics/report",
    "case-report": "info:eu-repo/semantics/report",
    "correction": "info:eu-repo/semantics/other",
    "editorial": "info:eu-repo/semantics/other",
    "in-brief": "info:eu-repo/semantics/other",
    "letter": "info:eu-repo/semantics/other",
    "other": "info:eu-repo/semantics/other",
    "partial-retraction": "info:eu-repo/semantics/other",
    "rapid-communication": "info:eu-repo/semantics/other",
    "reply": "info:eu-repo/semantics/other",
    "retraction": "info:eu-repo/semantics/other",
    "review-article": "info:eu-repo/semantics/article",
}


def fetch_pubtype_from_vocabulary(typ):
    return ARTICLETYPE_TO_VOCABULARY_MAP.get(typ, "info:eu-repo/semantics/other")


class OAIRecord:
    def __init__(self, data, context):
        self.data = data
        self._context = context

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
                "type": [fetch_pubtype_from_vocabulary(self.data.get("type"))],
                "format": ["text/html"],
                "identifier": [
                    self._context["url_for_html"](
                        acron=self.data["journal_acron"], doc_id=self.data["doc_id"],
                    ),
                ],
                "source": [],
                "language": self._language(),
                "relation": self._relation(),
                "rights": ["info:eu-repo/semantics/openAccess"],
            },
        )

    def _title(self):
        return [
            {"text": i.get("title", ""), "lang": i.get("lang", "")}
            for i in self.data.get("titles", {})
        ]

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
        return [
            {"text": i["kwd"].title(), "lang": i.get("lang", "")}
            for i in self.data.get("keywords", [])
            if i.get("kwd")
        ]

    def _description(self):
        return [
            {"text": i["description"], "lang": i.get("lang", "")}
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
