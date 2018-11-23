package main.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object CreateDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Word Count")
      .getOrCreate()

    val dataFrame = spark.read
      .option("header", true)
      .option("nullValue", "NA")  // 只能有一组 nullValue
      .csv("file:\\C:\\Users\\jiangyilan\\IdeaProjects\\Titanic_And_House_Price\\data\\HousePrice\\train.csv")

    dataFrame.show()

    spark.stop()
  }
}
