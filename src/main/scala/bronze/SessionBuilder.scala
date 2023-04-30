package bronze

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/** Provides functions to build a Spark Session. */
object SessionBuilder {

  /** Creates the Spark Session.
   *
   * @param appName the app name
   * @param env the environment which you are working
   * @return SparkSession object
   */
  def createSession(appName: String, env: String): SparkSession = {

    val conf = new SparkConf()
    val settings: Map[String, String] = Map(
      "spark.app.name" -> appName,
      "spark.sql.catalog.prod" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.prod.warehouse" -> "s3://platform-event-datalake/warehouse",
      "spark.sql.catalog.prod.catalog-impl" -> "org.apache.iceberg.aws.glue.GlueCatalog",
      "spark.sql.catalog.prod.io-impl" -> "org.apache.iceberg.aws.s3.S3FileIO",
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    conf.setAll(settings)

    val session = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    session
  }

}
