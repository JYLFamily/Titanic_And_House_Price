package main.spark.rdd

import org.apache.spark.sql.SparkSession

object RddTest3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("RddTest3")
      .getOrCreate()

    val fileOne = spark
      .sparkContext
      .parallelize(List[Int](129, 54, 167, 324, 111, 54, 26, 697, 4856, 3418))

    val fileTwo = spark
      .sparkContext
      .parallelize(List[Int](5, 329, 14, 4567, 2186, 457, 35, 267))

    val fileAll = fileOne.union(fileTwo)

    // 最小
    fileAll
      .map(element => (element, 1))
      .sortByKey()
      .take(1)
      .foreach(element => println(element._1))

    // 最大
    fileAll
      .map(element => (element, 1))
      .sortByKey(false)
      .take(1)
      .foreach(element => println(element._1))

    println("------------------------------------")

    // 最小
    val min = fileAll
      .reduce((a, b) => if (a > b) b else a)

    // 最大
    val max = fileAll
      .reduce((a, b) => if (a > b) a else b)

    println(min)
    println(max)

    println("------------------------------------")

    // 同时得到最小、最大
    fileAll
        .map(element => ("key", element))
        .groupByKey()
        .map(element => {
          var min = Integer.MAX_VALUE
          var max = Integer.MIN_VALUE

          for (i <- element._2) {
            if (i > max) {
              max = i
            }

            if (i < min) {
              min = i
            }
          }

          (min, max)
        })
       .map(element => {
         println("min: " + element._1)
         println("max: " + element._2)
       }).collect()

    spark.stop()
  }
}
