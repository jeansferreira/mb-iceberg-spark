package utils

import bronze.SessionBuilder
import bronze.pojo._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Calendar

/** Creates table at Bronze Layer based on getSchema from event pojo.
 * Very useful when you have to create tables with too many columns and don't want to deal with
 * type-to-type problems.
 *
 * */
object IcebergTableCreator {

  def main(args: Array[String]): Unit = {

    val programArguments = ArgumentsParser.parseIcebergTableCreator(
      args,
      "IcebergTableCreator"
    )
    val event = programArguments.event
    val pojo = returnEventPojo(event)

    val spark: SparkSession = SessionBuilder
      .createSession("IcebergTableCreator", "prod")

    val df = spark
      .read 
      .format("kafka")
      .option("kafka.bootstrap.servers", pojo.kafkaBrokers)
      .option("subscribe", "person")
      .option("failOnDataLoss", false)
      .option("endingOffsets", "latest")
      .option("kafka.group.id", "mb-kafka-iceberg-person")
      .load()

    import spark.implicits._

    val jsonDF = df.selectExpr("CAST(value AS STRING)")
    val finalDF = spark.read.schema(pojo.getSchema).json(jsonDF.as[String])

    spark.sql(s"""CREATE DATABASE IF NOT EXISTS prod.${pojo.icebergDB}""")

   val today   = Calendar.getInstance()
   val Day    = today.get(Calendar.DATE ).toInt
   val Year    = today.get(Calendar.YEAR ).toInt
   val Month1  = today.get(Calendar.MONTH ).toInt
   val Month   = (Month1+1).toInt
   val Hour   = today.get(Calendar.HOUR_OF_DAY).toInt

   finalDF.withColumn("year", lit(Year))
   finalDF.withColumn("month", lit(Month))
   finalDF.withColumn("day", lit(Day))
   finalDF.withColumn("hour", lit(Hour))

   finalDF.printSchema()

   // Gravar os no S3 via Iceberg.
    
    finalDF
      .withColumn("year", date_format(col("timestamp"), "yyyy").cast("int"))
      .withColumn("month", date_format(col("timestamp"), "MM").cast("int"))
      .withColumn("day", date_format(col("timestamp"), "dd").cast("int"))
      .withColumn("hour", date_format(col("timestamp"), "HH").cast("int"))
      .writeTo(s"""prod.${pojo.icebergDB}.${pojo.icebergTable}""")
      .tableProperty("format-version", "2")
      .partitionedBy(col("year"), col("month"), col("day"), col("hour"))
      .createOrReplace()
  }

  /**
   * Return the event pojo.
   * @param eventName the event name referring to the pojo
   */
  def returnEventPojo(eventName: String): Event = {
    eventName match {
      case "person" => Person
      case _ => throw new IllegalArgumentException(s"""Evento ${eventName} não existe.""")
    }
  }

}