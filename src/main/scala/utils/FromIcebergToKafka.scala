package utils

import bronze.SessionBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.LocalDateTime

object FromIcebergToKafka {

  def main(args: Array[String]) = {

    val spark: SparkSession = SessionBuilder
      .createSession("FromIcebergToKafka", "prod")

    import spark.implicits._

    val dt_integration = spark.sql("SELECT dtintegration FROM prod.bronze.integration where tablename = 'person'").first()
    val dtintegration = dt_integration.get(0)

    val d = LocalDateTime.now
    val f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val ldt = f.format(d)

    val sql = ("SELECT * FROM prod.bronze.person where date >= '"+dtintegration+"' and date <= '"+ldt+"'")
    val df = spark.sql(sql)

    val kafkaBrokers = "localhost-kafka-1:9092," + 
                       "localhost-kafka-2:9092"

    val sql_up = ("UPDATE prod.bronze.integration SET dtintegration = '"+ldt+"' where tablename = 'person'")
    spark.sql(sql_up)

    df.select(to_json(struct("*")).alias("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", "person")
      .save()

    print("Processo finalizado!")
  }

}
