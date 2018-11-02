package main.spark.rdd

import org.apache.spark.sql.SparkSession

object PairRddByKey {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("PairRddByKey")
      .getOrCreate()

    val myRdd = spark
      .sparkContext
      .parallelize(List[(String, Int)](("Spark", 1), ("Spark", 2), ("Hadoop", 3), ("Hadoop", 4)))

//    RDD[(String, scala.Iterable[Int])
    myRdd
      .groupByKey()
      .foreach(element => println(element._2.getClass))

    spark.stop()
  }
}
