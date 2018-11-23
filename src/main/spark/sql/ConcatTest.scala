package main.spark.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ConcatTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("PipelineTest")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // rdd
    val rdd = spark.sparkContext.parallelize(List[String]("Jack, 15, 1111", "Andy, 30, 1111"))
    val rddRow = rdd.map(element => element.split(", "))
      .map(element => Row(element(0), element(1).trim, element(2).trim))

    // data frame
    val fields = Array[StructField](
      StructField("name", StringType, nullable = true),
      StructField("school_id", StringType, nullable = true),
      StructField("student_id", StringType, nullable = true)
    )
    val schemaFields = StructType(fields)
    val dataFrame = spark.createDataFrame(rddRow, schemaFields)

    // concat concat_ws
    dataFrame.select(concat(dataFrame("school_id"), dataFrame("school_id"))).show
    println("*" * 36)
    dataFrame.select(concat(dataFrame("school_id"), lit("_"), dataFrame("school_id"))).show
    println("*" * 36)
    dataFrame.select(concat_ws("_", dataFrame("school_id"), dataFrame("school_id"))).show
    spark.stop()
  }
}
