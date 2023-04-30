package utils

import bronze.SessionBuilder
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.spark.sql.SparkSession
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier

import scala.collection.JavaConverters._

/** Task required for cleaning old snapshots from Iceberg Tables. */
object MaintenanceTask {

  def main(args: Array[String]): Unit = {

    val programArguments = ArgumentsParser.parseMaintenanceTaskArgs(
      args,
      "MaintenanceTask"
    )
    val tableName = programArguments.table
    val databaseName = programArguments.database

    val spark: SparkSession = SessionBuilder.createSession("MaintenanceTask", "prod")

    val catalog = new GlueCatalog()
    catalog.setConf(spark.sparkContext.hadoopConfiguration)

    val properties: Map[String, String] = Map("warehouse" -> "s3://aqrl-mb-datalake/warehouse")

    catalog.initialize("prod", properties.asJava)

    val tableID: TableIdentifier = TableIdentifier.of(databaseName, tableName)

    val table: Table = catalog.loadTable(tableID)

    val tsToExpire: Long = System.currentTimeMillis() - (1000 * 60 * 60 * 24) // 1 dia

    table.expireSnapshots()
      .expireOlderThan(tsToExpire)
      .commit()

  }

}
