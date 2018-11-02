package main.spark.rdd

import org.apache.spark.sql.SparkSession

object PairRddTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("PairRddTest")
      .getOrCreate()

    val pairRdd = spark.sparkContext.parallelize(List[(String, Int)](
      ("spark", 2),
      ("hadoop", 6),
      ("hadoop", 4),
      ("spark", 6)
    ))

//    此实现不具有推广性
//    pairRdd
//      .mapValues(element => element / 2)
//      .reduceByKey((a, b) => a + b)
//      .foreach(element => println(element))

    pairRdd
      .mapValues(value => (value, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))  // (图书数量, 图书品类)
      .mapValues(x => x._1 / x._2)
      .foreach(element => println(element))

    spark.stop()
  }
}
