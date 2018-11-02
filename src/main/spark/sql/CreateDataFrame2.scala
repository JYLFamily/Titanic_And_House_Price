package main.spark.sql

import org.apache.spark.sql.SparkSession


object CreateDataFrame2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()

    import spark.implicits._

    val rdd = spark.sparkContext.parallelize(List[(String, Int)](("Jack", 15), ("Mike", 26)))
    // DataSet[Row] or DataFrame
    val dataFrame = rdd
        .map(tuple => Human(tuple._1, tuple._2))
        .toDF()
    dataFrame.show

    // DataSet[String]
    val dataSet = dataFrame.map(row => "Name: " + row(0) + " " + "Age: " + row(1))
    dataSet.show

    spark.stop()
  }
}
