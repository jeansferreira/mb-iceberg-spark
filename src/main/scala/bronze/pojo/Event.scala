package bronze.pojo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/** Interface that all events must implement. */
trait Event {

  /** Which topic the event is present. */
  def topic: String

  /** The catalog to which the event goes. */
  def icebergCatalog: String

  /** The database to which the event goes. */
  def icebergDB: String

  /** The table to which the event goes. */
  def icebergTable: String

  /** The Kafka Cluster's Brokers where the event is coming from. */
  def kafkaBrokers: String

  /** Return the event's table schema. */
  def getSchema: StructType

}
