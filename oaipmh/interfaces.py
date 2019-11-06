from typing import Iterable, Dict, ByteString


class Tasks:
    """Sequência de tarefas que devem ser desempenhadas a fim de que os dados
    sejam replicados.

    As tarefas são segregadas entre os documentos que devem ser obtidos e os 
    que devem ser removidos. O `timestamp` da última mudança lida pode ser 
    acessado no atributo de mesmo nome.
    """

    def __init__(self, tasks, timestamp):
        self.tasks = tasks
        self.timestamp = timestamp

    def docs_to_get(self) -> Iterable[Dict]:
        """Referências aos documentos que devem ser obtidos do DB remoto.
        """

    def docs_to_del(self) -> Iterable[Dict]:
        """Referências aos documentos que devem ser removidos do DB local.
        """


class TasksReader:
    """Produz uma sequência de tarefas a partir dos registros dos eventos de 
    mudança da fonte de dados remota.

    Idealmente as tarefas reduzem os eventos relacionados a cada documento de 
    forma que seja possível replicar apenas seu último estado, p. ex., se o 
    *DOC-A* foi criado, alterado e removido minutos depois, não deverá haver
    tarefas para a obtenção do registro mas apenas para garantir que esteja 
    removido.
    """

    def read(self, changelog: Iterable[Dict]) -> Tasks:
        """Produz uma sequência de tarefas a partir dos registros dos eventos de 
        mudança da fonte de dados remota.
        """


class DataConnector:
    """Representa a conexão com o banco de dados remoto a ser replicado.

    Este DB remoto deve oferecer uma maneira do cliente da replicação ter 
    acesso aos registros dos eventos de mudança dos registros.
    """

    def changes(since: str = "") -> Iterable[Dict]:
        """Sequência ordenada dos registros de eventos de mudança. 
        """

    def fetch_document(doc_id: str) -> ByteString:
        """Fetch the full data for `doc_id`.
        """

    def fetch_metadata(doc_id: str) -> Dict:
        """Fetch metadata for `doc_id`.
        """
