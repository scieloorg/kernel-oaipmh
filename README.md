# OAI-PMH Data Provider

Provedor de dados OAI-PMH parte da suíte de aplicativos da Metodologia SciELO
(_SciELO Publishing Framework_).

Principais objetivos:

* Suportar por completo o protocolo [OAI-PMH versão 2.0](https://www.openarchives.org/OAI/openarchivesprotocol.html);
* Distribuir [XMLs SciELO PS](https://scielo.readthedocs.io/projects/scielo-publishing-schema/) (JATS);
* Flexibilidade na criação de novos _sets_ e formatos de metadados;
* Integração com o _Kernel_, considerado fonte autoritativa dos dados da coleção.

Para mais informação sobre a nova arquitetura de sistemas de informação da
Metodologia SciELO consulte https://docs.google.com/document/d/14YBl7--4ouaWBQhxzUYWRuhmegwnSYrDgupsED6rhvM/edit?usp=sharing


## Requisitos

* Python 3.7
* MongoDB


## Implantação local

Configurando a aplicação:


diretiva no arquivo .ini         | variável de ambiente             | valor padrão
---------------------------------|----------------------------------|--------------------
oaipmh.mongodb.dsn               | OAIPMH_MONGODB_DSN               | mongodb://db:27017
oaipmh.mongodb.replicaset        | OAIPMH_MONGODB_REPLICASET        |
oaipmh.mongodb.readpreference    | OAIPMH_MONGODB_READPREFERENCE    | secondaryPreferred
oaipmh.repo.name                 | OAIPMH_REPO_NAME                 | SciELO - Scientific Electronic Library Online
oaipmh.repo.baseurl              | OAIPMH_REPO_BASEURL              | http://www.scielo.br/oai/scielo-oai.php
oaipmh.repo.protocolversion      | OAIPMH_REPO_PROTOCOLVERSION      | 2.0
oaipmh.repo.adminemails          | OAIPMH_REPO_ADMINEMAILS          | scielo@scielo.org
oaipmh.repo.deletedrecord        | OAIPMH_REPO_DELETEDRECORD        | no
oaipmh.repo.granularity          | OAIPMH_REPO_GRANULARITY          | YYYY-MM-DDThh:mm:ssZ
oaipmh.repo.compression          | OAIPMH_REPO_COMPRESSION          | identity
oaipmh.resumptiontoken.batchsize | OAIPMH_RESUMPTIONTOKEN_BATCHSIZE | 100
oaipmh.site.baseurl              | OAIPMH_SITE_BASEURL              | https://www.scielo.br


A configuração padrão assume o uso de uma instância *standalone* do MongoDB. Para
uma instância de produção recomenda-se o uso de *replica sets*. Para mais detalhes
acesse https://docs.mongodb.com/manual/replication/.

Ao conectar-se a um *replica set*, a diretiva `oaipmh.mongodb.replicaset`
deve ser definida com o nome do *replica set*. Além disso, é possível informar os diversos
*seeds* do *replica set* por meio da diretiva `oaipmh.mongodb.dsn`,
separando suas URIs com espaços em branco ou quebra de linha.


Configurações avançadas:


variável de ambiente    | valor padrão
------------------------|-------------
OAIPMH_MAX_RETRIES      | 4
OAIPMH_BACKOFF_FACTOR   | 1.2
OAIPMH_HTTP_REQ_TIMEOUT | 5


### Executando via código-fonte e Pip:

```bash
$ git clone https://github.com/scieloorg/kernel-oaipmh.git
$ cd kernel-oaipmh
$ pip install -r requirements.txt && python setup.py develop
$ pserve development.ini
```

Esta configuração espera uma instância de MongoDB escutando *localhost* na
porta *27017*.

Na primeira vez será necessário criar os índices do banco de dados. Para tal
execute o comando `oaipmhctl create-indexes`*`mongo-db-dsn`*. Exemplo:

```bash
$ oaipmhctl create-indexes mongodb://localhost:27017
```


Para sincronizar o banco de dados da aplicação com o de uma instância do
_Kernel_, execute o comando `oaipmhctl sync`*`source-url mongo-db-dsn`*. Exemplo:

```bash
$ oaipmhctl sync http://my-kernel:6543 mongodb://localhost:27017
```


### Executando via Docker:

`$ docker-compose up -d`

Na primeira vez será necessário criar os índices do banco de dados e
sincronizar o banco de dados:

`$ docker-compose exec webapp_oaipmh oaipmhctl create-indexes`*`mongo-db-dsn`*

`$ docker-compose exec webapp_oaipmh oaipmhctl sync`*`source-url mongo-db-dsn`*


A sincronização deverá ser executada periodicamente, de forma a manter o provedor
de dados OAI-PMH sincronizado com a fonte autoritativa de dados. O utilitário de
sincronização cuidará das informações relativas ao estado da sincronização, de
forma que o usuário não necessitará controlar _timestamps_ ou coisas do tipo.


Para testar se a instância foi instalada corretamente basta executar:

```bash
curl -X GET http://0.0.0.0:6543/?verb=Identify
```


## Licença de uso

Copyright 2020 SciELO <scielo-dev@googlegroups.com>. Licensed under the terms
of the BSD license. Please see LICENSE in the source code for more
information.

https://github.com/scieloorg/kernel-oaipmh/blob/master/LICENSE
