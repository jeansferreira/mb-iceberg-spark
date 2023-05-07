package utils

import scopt.OptionParser

case class IcebergTableCreatorArguments(
                              event: String = ""
                              )

case class FromKafkaToIcebergArguments(
                                      event: String = ""
                                      )

case class PrintSchemaArguments(
                                 topic: String = "",
                                 kafkaBrokers: String = "localhost-kafka-1:6667," +
                                                        "localhost-kafka-2:6667"
                               )

case class MaintenanceTaskArguments(
                                   table: String = "",
                                   database: String = ""
                                   )

/** Provides parsers to read arguments for Spark jobs. */
object ArgumentsParser {

  def parseFromKafkaToIcebergArgs(args: Seq[String], jobName: String): FromKafkaToIcebergArguments = {

    val parseFromKafkaToIceberg = new OptionParser[FromKafkaToIcebergArguments](jobName) {
      head("Job usage")

      opt[String]('e', "event").action((x, c) =>
        c.copy(event = x)).text("Event to read from Kafka and write to Iceberg Tables").required()
    }

    parseFromKafkaToIceberg.parse(args, FromKafkaToIcebergArguments()) match {
      case Some(arguments) => arguments
      case None => throw new IllegalArgumentException()
    }

  }

  def parseIcebergTableCreator(args: Seq[String], jobName: String): IcebergTableCreatorArguments = {

    val parseIcebergTableCreator = new OptionParser[IcebergTableCreatorArguments](jobName) {
      head("Job usage")

      opt[String]('e', "event").action((x, c) =>
      c.copy(event = x)).text("Catalog which the table will be created.").required()
    }

    parseIcebergTableCreator.parse(args, IcebergTableCreatorArguments()) match {
      case Some(arguments) => arguments
      case None => throw new IllegalArgumentException()
    }
  }

  def parsePrintSchemaArgs(args: Seq[String], jobName: String): PrintSchemaArguments = {
    val printSchemaParser = new OptionParser[PrintSchemaArguments](jobName) {
      head("Job usage")

      opt[String]('t', "topic").action((x, c) =>
        c.copy(topic = x)).text("Topic to read from.").required()

      opt[String]('k', "kafkaBrokers").action((x, c) =>
        c.copy(kafkaBrokers = x)).text("List of Kafka Brokers").optional()
    }

    printSchemaParser.parse(args, PrintSchemaArguments()) match {
      case Some(arguments) => arguments
      case None => throw new IllegalArgumentException
    }
  }

  def parseMaintenanceTaskArgs(args: Seq[String], jobName: String): MaintenanceTaskArguments = {
    val maintenanceTaskParser = new OptionParser[MaintenanceTaskArguments](jobName) {
      head("Job usage")

      opt[String]('t', "table").action((x, c) =>
        c.copy(table = x)).text("The table that being maintained.").required()

      opt[String]('d', "database").action((x, c) =>
        c.copy(database = x)).text("The database where the table belongs.").required()

    }

    maintenanceTaskParser.parse(args, MaintenanceTaskArguments()) match {
      case Some(arguments) => arguments
      case None => throw new IllegalArgumentException
    }
  }

}


