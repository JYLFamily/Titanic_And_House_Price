package main.spark.rdd

import org.apache.spark.sql.SparkSession

object RddTest5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("RddTest4")
      .getOrCreate()

    val file = spark
      .sparkContext
      .parallelize(List[(Int, Int)]((5, 3), (1, 6), (4, 9), (8, 3), (5, 7), (5, 6), (3, 2)))

    file
      .map(element => (new SecondarySortKey(element._1, element._2), element))
      .sortByKey(false)
      .map(element => (element._2._1, element._2._2))
      .foreach(element => println(element))

    spark.stop()
  }
}
