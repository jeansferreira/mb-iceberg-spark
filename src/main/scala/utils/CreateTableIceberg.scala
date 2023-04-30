
package utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CreateTableIceberg {

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val settings: Map[String, String] = Map(
      "spark.app.name" -> "Aqrl-MB-Job",
      "spark.sql.catalog.prod" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.prod.warehouse" -> "/dev/mnt/datatalake", //Ex.; "s3://aqrl-mb-datalake/warehouse"
      "spark.sql.catalog.prod.catalog-impl" -> "org.apache.iceberg.aws.glue.GlueCatalog",
      "spark.sql.catalog.prod.io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    conf.setAll(settings)

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    spark.sql("CREATE DATABASE IF NOT EXISTS prod.bronze")

    val rdd = spark.sparkContext.parallelize(Seq((1, "João", "Masculino"), (2, "Maria", "Feminino")))
    val df = rdd.toDF("ID", "Nome", "Sexo")

    df.writeTo("bronze.person")
      .tableProperty("format-version", "2")
      .createOrReplace()

  }
}
