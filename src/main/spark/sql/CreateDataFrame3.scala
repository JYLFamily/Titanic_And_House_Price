package main.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object CreateDataFrame3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(List[String]("Jack, 15", "Andy, 30"))
    val rddRow = rdd.map(element => element.split(", "))
      .map(element => Row(element(0), element(1).trim.toInt))

    val schema = Array[String]("name", "age")
    val fields = Array[StructField](
      StructField(schema(0), StringType, nullable = true),
      StructField(schema(1), IntegerType, nullable = true)
    )
    val schemaFields = StructType(fields)

    val dataFrame = spark.createDataFrame(rddRow, schemaFields)
    dataFrame.show

    spark.stop()
  }
}
