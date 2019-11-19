# OAI-PMH Data Provider

Provedor de dados OAI-PMH parte da suíte de aplicativos da Metodologia SciELO
(_SciELO Publishing Framework_).

Principais objetivos:

* Suportar por completo o protocolo [OAI-PMH versão 2.0](https://www.openarchives.org/OAI/openarchivesprotocol.html);
* Operar nativamente com [XMLs SciELO PS](https://scielo.readthedocs.io/projects/scielo-publishing-schema/) (JATS);
* Flexibilidade na criação de novos _sets_ e formatos de metadados;
* Disponibilizar formato de metadado `oai_jats`, onde serão entregues os XMLs
  em texto completo e codificados de acordo com a especificação SciELO PS (JATS).


Para mais informação sobre a nova arquitetura de sistemas de informação da
Metodologia SciELO consulte https://docs.google.com/document/d/14YBl7--4ouaWBQhxzUYWRuhmegwnSYrDgupsED6rhvM/edit?usp=sharing

## Requisitos

* Python 3.7+
* MongoDB
