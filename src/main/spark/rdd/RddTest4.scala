package main.spark.rdd

import org.apache.spark.sql.SparkSession

object RddTest4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("RddTest4")
      .getOrCreate()

    val fileOne = spark
      .sparkContext
      .parallelize(List[Int](33, 37, 12, 40))

    val fileTwo = spark
      .sparkContext
      .parallelize(List[Int](4, 16, 39, 5))

    val fileThree = spark
      .sparkContext
      .parallelize(List[Int](1, 45, 25))

    val fileAll = fileOne
      .union(fileTwo)
      .union(fileThree)

    var num: Int = 0
    fileAll
      .map(element => (element, 1))
      .sortByKey()
      .map(element => {
        element._1
      })
      .repartition(1)
      .foreach(
        element => {
          num += 1
          println(num + "\t" + element)
        }
      )


  }
}
