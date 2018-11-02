package main.spark.rdd

import org.apache.spark.sql.SparkSession

object PairRddCreate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("PairRddCreate")
      .getOrCreate()

    val pairRdd = spark
      .sparkContext
      .parallelize(List[String]("I", "Love", "Hadoop"))
      .map(word => (word, 1))

    println(pairRdd.getClass)
    pairRdd.foreach(tuple => println(tuple))

    spark.stop()
  }
}
