from collections import namedtuple
from typing import Type, TypeVar, Iterable


# serve apenas para type-hinting:
TResumptionToken = TypeVar("TResumptionToken", bound="ResumptionToken")


TOKEN_FIELDS_SEPARATOR = ","


class ResumptionToken:
    attrs = ["set", "from_", "until", "offset", "count", "metadataPrefix"]

    def __init__(self, **kwargs):
        for attr in self.attrs:
            setattr(self, attr, kwargs.get(attr, None))

    @classmethod
    def decode(cls: Type[TResumptionToken], token: str, **kwargs) -> TResumptionToken:
        """Retorna uma instância de `cls` a partir da sua forma codificada.
        """
        keys = cls.attrs
        values = token.split(TOKEN_FIELDS_SEPARATOR)
        token_map = dict(zip(keys, values))
        return cls(**token_map, **kwargs)

    def encode(self) -> str:
        """Codifica o token em string delimitada por `TOKEN_FIELDS_SEPARATOR`.

        Durante a codificação, todos os valores serão transformados em `str`.
        `None` será transformado em string vazia.
        É importante ter em mente que o processo de codificação faz com que os
        tipos originais dos valores sejam perdidos, i.e., não é um processo
        reversível.
        """

        def ensure_str(obj):
            if obj is None:
                return ""
            else:
                try:
                    return str(obj)
                except:
                    return ""

        token = [getattr(self, attr) for attr in self.attrs]
        parts = [ensure_str(part) for part in token]
        return TOKEN_FIELDS_SEPARATOR.join(parts)

    def next(self, resources: Iterable) -> TResumptionToken:
        """Retorna o próximo resumption token com base na análise dos dados
        retornados junto com este.
        """
        if self._has_more_resources(resources, self.count):
            token_map = self._asdict()
            token_map["offset"] = self._incr_offset(resources)
            return self.__class__(**token_map)
        else:
            return None

    def _incr_offset(self, resources) -> TResumptionToken:
        """Avança o offset do token.
        """
        return str(resources[-1]["_id"])

    def _has_more_resources(self, resources: Iterable, batch_size: int) -> bool:
        """Verifica se `resources completa a lista de recursos.

        Se a quantidade de itens em `resources` for menor do que `batch_size,
        consideramos se tratar do último conjunto de resultados. Caso a
        quantidade seja igual, é considerado que existirá o próximo conjunto.
        """
        return len(resources) == int(batch_size)

    def _asdict(self) -> dict:
        return {attr: getattr(self, attr) for attr in self.attrs}

    def __repr__(self):
        return "<%s with values %s>" % (self.__class__.__name__, self._asdict())
