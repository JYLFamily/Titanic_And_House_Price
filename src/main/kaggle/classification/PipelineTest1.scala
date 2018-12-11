package main.kaggle.classification

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.DoubleType

class PipelineTest1 {
  private val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("PipelineTest1")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  private var trainDataFrame: DataFrame = _
  private var trainDataSet: Dataset[Sample] = _

  def readData(): Unit = {
    this.trainDataFrame = spark
      .read
      .schema(Encoders.product[Sample].schema)
      .option(key = "header", value = true)
      .option(key = "nullValue", value = "")
      .option(key = "inferSchema", value = true)
      .csv("data/Titanic/train.csv")
    this.trainDataSet = this.trainDataFrame.as[Sample]
  }

  def prepareData(): Unit = {
    this.trainDataSet
      .groupByKey(sample => sample.Pclass -> sample.Embarked)
      .agg(sum("Survived").as[Double])  // 需要 Typed Column, 但是 as 没有效果
      .map{ case ((i, j), k) => MiniSample(i, j, k)}
      .show
  }
}

object PipelineTest1 {
  def main(args: Array[String]): Unit = {
    val pt1: PipelineTest1 = new PipelineTest1()
    pt1.readData()
    pt1.prepareData()
  }
}
