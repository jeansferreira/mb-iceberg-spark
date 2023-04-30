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

    val dt_integration = spark.sql("SELECT dtintegration FROM prod.bronze.integration where tablename = 'pageviews'").first()
    val dtintegration = dt_integration.get(0)

    val d = LocalDateTime.now
    val f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val ldt = f.format(d)

    val sql = ("SELECT * FROM prod.bronze.pageviews where date >= '"+dtintegration+"' and date <= '"+ldt+"'")
    val df = spark.sql(sql)

    val kafkaBrokers = "kafka-01.event.linximpulse.net:9092," + 
                       "kafka-02.event.linximpulse.net:9092," + 
                       "kafka-03.event.linximpulse.net:9092," + 
                       "kafka-04.event.linximpulse.net:9092," + 
                       "kafka-05.event.linximpulse.net:9092," + 
                       "kafka-06.event.linximpulse.net:9092"

    val sql_up = ("UPDATE prod.bronze.integration SET dtintegration = '"+ldt+"' where tablename = 'pageviews'")
    spark.sql(sql_up)

    df.select(to_json(struct("*")).alias("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", "pageviews")
      .save()

    print("Processo finalizado!")
  }

}
