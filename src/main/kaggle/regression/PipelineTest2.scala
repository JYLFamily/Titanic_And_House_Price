package main.kaggle.regression

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class PipelineTest2(private val inputPath: String, private val outputPath: String) {
  private val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("PipelineTest2")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  private var train: DataFrame = _

  def readData(): Unit = {
    this.train = this.spark.read
      .option(key = "header", value = true)
      .option(key = "inferSchema", value = true)
      .csv(this.inputPath + "train.csv")

    this.train.select("LotFrontage")
      .na
      .replace("LotFrontage", Map[String, String]("NA" -> "0"))
      .withColumn("LotFrontageTemp", this.train("LotFrontage").cast(DoubleType))
      .drop("LotFrontage")
      .withColumnRenamed("LotFrontageTemp", "LotFrontage")

  }
}

object PipelineTest2 {
  def main(args: Array[String]): Unit = {
    val pt2: PipelineTest2  = new PipelineTest2("data/HousePrice/",  null)
    pt2.readData()
  }
}
