package main.spark.rdd

import org.apache.spark.sql.SparkSession

object RddTest2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("RddTest2")
      .getOrCreate()

    val fileOne = spark.sparkContext.parallelize(
      List[(Int, Int, Int, Int)](
        (1, 1768, 50, 155),
        (2, 1218, 600, 211),
        (3, 2239, 788, 242),
        (4, 3101, 28, 599),
        (5, 4899, 290, 129),
        (6, 3110, 54, 1201),
        (7, 4436, 259, 877),
        (8, 2369, 7890, 27)))

    val fileTwo = spark.sparkContext.parallelize(
      List[(Int, Int, Int, Int)](
        (100, 4287, 226, 233),
        (101, 6562, 489, 124),
        (102, 1124, 33, 17),
        (103, 3267, 159, 179),
        (104, 4569, 57, 125),
        (105, 1438, 37, 116)))

    val fileAll = fileOne.union(fileTwo)

    var num = 0
    fileAll
        .map(element => (element._3, 1))
        .sortByKey(false)
        .map(element => element._1)
        .take(5)  // take 返回 Array[Int] 就不要 reparation
        .foreach(element => {
          num = num + 1
          println(num + "\t" + element)
        })

    spark.stop()
  }
}
