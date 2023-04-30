package utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Print schema of a given Kafka topic.
 * Useful when you need to write the getSchema of a new event.
 * Use it. It's hard to know precisely the schema of a Kafka topic since many clients send different messages.
 * With this job you read all the messages present on this topic and Spark infer a schema for you. Don't copy it.
 * Instead, build a schema based on it to satisfy your needs.
 */
object PrintSchema {

  def main(args: Array[String]): Unit = {

    val programArguments = ArgumentsParser.parsePrintSchemaArgs(args, "Print Schema")
    val topic = programArguments.topic
    val kafkaBrokers = programArguments.kafkaBrokers

    val spark = buildSession("PrintSchema")

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", topic)
      .option("failOnDataLoss", false)
      .option("endingOffsets", "latest")
      .option("kafka.group.id", "mb-kafka-iceberg-"+topic)
      .load()

    import spark.implicits._

    val jsonDF = df.selectExpr("CAST(value AS STRING)")
    val finalDF = spark.read.json(jsonDF.as[String])

    finalDF.printSchema()

  }

  /** Builds a simple SparkSession.
   *
   * @param appName the app name
   * @return SparkSession object
   */
  def buildSession(appName: String): SparkSession = {

    val conf = new SparkConf()
      .setAppName(appName)

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    spark
  }

}
