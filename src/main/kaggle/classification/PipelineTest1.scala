package main.kaggle.classification

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class PipelineTest1 {
  private val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("PipelineTest")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  private var trainDataFrame: DataFrame = _
  private var testDataFrame: DataFrame = _
  private var trainDataSet: Dataset[Sample] = _
  private var testDataSet: Dataset[Sample] = _

  def readData(): Unit = {
    this.trainDataFrame = spark
      .read
      .option(key = "header", value = true)
      .option(key = "nullValue", value = "")
      .option(key = "inferSchema", value = true)
      .csv("data//Titanic//train.csv")
    this.trainDataSet = this.trainDataFrame.as[Sample]

    this.testDataFrame = spark
      .read
      .option(key = "header", value = true)
      .option(key = "nullValue", value = "")
      .option(key = "inferSchema", value = true)
      .csv("data//Titanic//train.csv")
    this.testDataSet = this.testDataFrame.as[Sample]

    this.trainDataSet.show
    this.testDataSet.show
  }
}

object PipelineTest1 {
  def main(args: Array[String]): Unit = {
    val pt1: PipelineTest1 = new PipelineTest1()
    pt1.readData()
  }
}
