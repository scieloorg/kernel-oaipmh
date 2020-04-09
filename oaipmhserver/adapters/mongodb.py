import logging

import pymongo

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
            {
                "$group": {
                    "_id": "$sets.set_spec",
                    "names": {"$push": "$sets.set_name"},
                }
            }
        ]
        return sorted(
            [
                {"set_spec": r["_id"][0], "set_name": r["names"][0][0]}
                for r in self._collection.aggregate(pipeline)
            ],
            key=lambda x: x["set_spec"],
        )
