package main.spark.rdd

import org.apache.spark.sql.SparkSession

object RddTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("RddTest")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    val list = List[String]("Hadoop is good", "Spark is fast", "Spark is better")
    val lines = sc.parallelize(list)
    val words = lines.flatMap(line => line.split(" "))
    words.foreach(word => println(word))
  }
}
