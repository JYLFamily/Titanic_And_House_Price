package main.spark.rdd

import org.apache.spark.sql.SparkSession

object RddCreate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("RddCreate")
      .getOrCreate()

    val path = "file:\\C:\\Users\\jiangyilan\\IdeaProjects\\Titanic_And_House_Price\\data\\HousePrice\\train.csv"
    val housePrice = spark.sparkContext.textFile(path)
      .map(line => line.split(","))
      .map(array => (array(0), array(1)))

//    #FIX jdk 10 报错 jdk 8 解决
//    val housePriceHead = housePrice.map(tuple => tuple._1).take(5)

    val housePriceHead = housePrice
        .map(tuple => tuple._2)
        .take(5)

    for (element <- housePriceHead) println(element)

    spark.stop()
  }
}
