# Aquarela - MB Spark Project

Tenha em mente que este ainda é um projeto em seus estágios iniciais e tem muito para amadurecer.

## Packages

Esta seção explica como foi projetada e o que contém cada um dos pacotes em `main/scala`.

### bronze

A ideia aqui é implementar tudo referente ao transporte dos dados do Kafka para a primeira camada da 
nova arquitetura, também chamada de bronze (por seguir a arquitetura de dados _medallion_), 
bem como os pojos dos eventos e o construtor do SparkSession.

Dentro de `/pojo` está uma _trait_ (interface em Scala) que contém tudo o que um novo evento deve implementar: Event. 
Novas funcionalidades de eventos devem ser adicionadas aqui para que outros eventos possam implementá-la. 
Ainda dentro de `/pojo` estão todas as implementações dessa _trait_. Até este momento, somente Transaction 
está implementada. Tome-a como exemplo.

Dentro deste pacote está também o objeto FromKafkaToIceberg.scala. É um _job_ Spark Streaming responsável por 
ler de um tópico do Kafka os eventos que chegam e escrevê-los em suas devidas tabelas Iceberg.

Ainda, SessionBuilder.scala é um objeto de construção de SparkSession.

**Observações importantes:**
- existe um _hard code_ extremamente feio neste pacote (eu estou ciente disso e já estava nos
planos corrigir. Não culpe o autor deste texto.) Observe a linha 20 de SessionBuilder.scala:

```scala
"spark.sql.catalog.prod" -> "org.apache.iceberg.spark.SparkCatalog",
```

Aqui, `prod` é o nome do catálogo. Espera-se que o herdeiro deste projeto faça os _jobs_ trabalharem com catálogos 
diferentes baseado no ambiente de desenvolvimento em questão.

A propósito...

- ... o Iceberg trabalha com a seguinte hierarquia: catálogo -> base de dados -> tabela. Observe 
estas linhas em FromKafkaToIceberg:

```scala
      .option("path",
        s"""${pojo.icebergCatalog}.
           |${pojo.icebergDB}.
           |${pojo.icebergTable}""".stripMargin)
```

Elas indicam que os dados vão para a tabela icebergTable, que pertence a base de dados icebergDB, 
que pertence ao catálogo icebergCatalog.

### utils

A ideia deste pacote é ter todas as implementações de códigos utilizado em todas as etapas ou que se faz presente 
poucas vezes em um fluxo (como é o caso de IcebergTableCreator).

## Build

Este projeto já está configurado para gerar uma imagem Docker com o Fat Jar da aplicação. O comando de build é 
`sbt docker`. Ele vai gerar uma imagem pronta para ir para o repositório do time. Tudo isso está
configurado no arquivo `build.sbt`.

#### Digite no prompt o seguinte comando:
```
$ sbt docker
```

### Outras fontes de material
- https://iceberg.apache.org/docs/latest/getting-started/
- https://www.dremio.com/blog/3-ways-to-use-python-with-apache-iceberg/
- https://github.com/apache/iceberg