package main.spark.ml

import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class PipelineTest2() {

  private val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("PipelineTest")
    .getOrCreate()

  private var train: DataFrame = null
  private var test: DataFrame = null

  private var tokenizer: Tokenizer = null
  private var hashingTF: HashingTF = null

  private var logisticRegression: LogisticRegression = null
  private var logisticRegressionModel: LogisticRegressionModel = null

  def dataRead(): Unit = {
    import spark.implicits._

    this.train = spark.sparkContext.parallelize(List[(Int, String, Double)](
      (0, "a b c d e spark", 1.0),
      (1, "spark f g h", 1.0),
      (2, "b d", 0.0),
      (3, "hadoop map", 0.0)))
      .map(tuple => PipelineTestData(tuple._1, tuple._2, tuple._3))
      .toDF("id", "text", "label")

    this.test = spark.sparkContext.parallelize(List[(Int, String, Double)](
      (0, "a b c d e spark", 1.0),
      (1, "spark f g h", 1.0),
      (2, "b d", 0.0),
      (3, "hadoop map", 0.0)))
      .map(tuple => PipelineTestData(tuple._1, tuple._2, tuple._3))
      .toDF("id", "text", "label")
  }

  def dataPrepare(): Unit = {
    this.tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("word")

    this.train = this.tokenizer.transform(this.train)
    this.test = this.tokenizer.transform(this.test)

    this.hashingTF = new HashingTF()
        .setNumFeatures(10)
        .setInputCol(this.tokenizer.getOutputCol)
        .setOutputCol("features")

    this.train = this.hashingTF.transform(this.train)
    this.test = this.hashingTF.transform(this.test)
  }

  def modelFitPredict(): Unit = {
    this.logisticRegression = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.01)

    this.logisticRegressionModel = this.logisticRegression.fit(this.train)
    this.logisticRegressionModel.transform(this.test)
      .select("probability")
      .foreach(element => println(element))
  }
}

object PipelineTest2 {
  def main(args: Array[String]): Unit = {
    val pt2: PipelineTest2 = new PipelineTest2()
    pt2.dataRead()
    pt2.dataPrepare()
    pt2.modelFitPredict()
  }
}
