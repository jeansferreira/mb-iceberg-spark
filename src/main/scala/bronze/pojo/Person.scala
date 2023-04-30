package bronze.pojo
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{ArrayType, DateType, DecimalType, IntegerType, DoubleType, LongType, MapType, StringType, StructField, StructType, TimestampType}

object Person extends Event {

  override def topic: String = "person"

  override def icebergCatalog: String = "prod"

  override def icebergDB: String = "bronze"

  override def icebergTable: String = "person"

  override def kafkaBrokers: String = "localhost:9092"

  override def getSchema: StructType = {

    val schema = StructType(List(
      StructField("ID", StringType, true),
      StructField("Nome", StringType, true),
      StructField("Sexo", StringType, true)
    ))
    schema
  }

}
