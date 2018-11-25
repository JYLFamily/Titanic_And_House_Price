package main.kaggle.regression

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{Bucketizer, QuantileDiscretizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PipelineTest2(private val inputPath: String, private val outputPath: String) {
  private val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("PipelineTest2")
      .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  private var train: DataFrame = _
  private val target: String = "SalePrice"

  def readData(): Unit = {
    this.train = this.spark
        .read
        .option(key = "header", value = true)
        .option(key = "inferSchema", value = true)
        .csv(this.inputPath + "train.csv")

    this.train = this.train
        .drop(this.train("Id"))

    this.train = this.train
        .na
        .replace("LotArea", Map[String, String]("NA" -> "0"))

    this.train = this.train
        .withColumn("LotFrontageTemp", this.train("LotFrontage").cast(DoubleType))
        .drop("LotFrontage")
        .withColumnRenamed("LotFrontageTemp", "LotFrontage")

//    val qCut: QuantileDiscretizer = new QuantileDiscretizer()
//        .setHandleInvalid("keep")
//        .setInputCol("LotFrontage")
//        .setNumBuckets(10)
//        .setOutputCol("LotFrontageBinning")
//    val qCutModel: Bucketizer = qCut
//        .fit(this.train)
//
//    val qCutModelDataFrame = qCutModel
//        .transform(this.train)
//        .select(qCut.getOutputCol)
//        .withColumn("Id", monotonically_increasing_id())
//    this.train = this.train
//        .withColumn("Id", monotonically_increasing_id())
//    this.train = this.train
//        .join(qCutModelDataFrame, Seq[String]("Id"), "inner")
//
//    this.train
//        .select(qCut.getOutputCol, this.target)
//        .groupBy(qCut.getOutputCol)
//        .agg(mean(this.target), count(this.target))
//        .orderBy(qCut.getOutputCol)
//        .show()

    println("*" * 36)

//    this.train.select("LotArea").dtypes.foreach(tuple => println(tuple._1 + " " + tuple._2))
//    val qCut: QuantileDiscretizer = new QuantileDiscretizer()
//      .setHandleInvalid("keep")
//      .setInputCol("LotArea")
//      .setNumBuckets(10)
//      .setOutputCol("LotAreaBinning")
//    val qCutModel: Bucketizer = qCut
//      .fit(this.train)
//
//    val qCutModelDataFrame = qCutModel
//      .transform(this.train)
//      .select(qCut.getOutputCol)
//      .withColumn("Id", monotonically_increasing_id())
//    this.train = this.train
//      .withColumn("Id", monotonically_increasing_id())
//    this.train = this.train
//      .join(qCutModelDataFrame, Seq[String]("Id"), "inner")
//
//    this.train
//      .select(qCut.getOutputCol, this.target)
//      .groupBy(qCut.getOutputCol)
//      .agg(mean(this.target), count(this.target))
//      .orderBy(qCut.getOutputCol)
//      .show()

    println("*" * 36)
//    this.train
//      .select($"BldgType", $"SalePrice" / $"LotArea" as "SalePricePerArea")
//      .groupBy($"BldgType")
//      .agg(mean("SalePricePerArea") as "mean", count("SalePricePerArea") as "count")
//      .orderBy("mean")
//      .show(100)

    println("*" * 36)
    this.train
        .select($"YrSold", $"MoSold")
        .withColumn("YrSoldTemp", $"YrSold".cast(StringType))
        .drop("YrSold")
        .withColumnRenamed("YrSoldTemp", "YrSold")
        .withColumn("MoSoldTemp", $"MoSold".cast(StringType))
        .drop("MoSold")
        .withColumnRenamed("MoSoldTemp", "MoSold")
        .withColumn(
          "MoSoldTemp",
          when(length($"MoSold") === lit(1), concat(lit("0"), $"MoSold")).otherwise($"MoSold")
        )
        .drop("MoSold")
        .withColumnRenamed("MoSoldTemp", "MoSold")
        .withColumn("YrMoSold", concat_ws(sep = "-", $"YrSold", $"MoSold", lit("01")))
        .limit(10)
        .withColumn("YrMoSoldDate", to_date($"YrMoSold", "yyyy-MM-dd"))
        .withColumn("Now", current_date())
        .withColumn("YrMoSoldDateNow", datediff(current_date(), $"YrMoSoldDate"))
        .show


//    this.train
//         .withColumn("MoSoldTemp", $"MoSold".cast(StringType))
//         .drop("MoSold")
//         .withColumnRenamed("MoSoldTemp", "MoSold")
//         .select($"MoSold", length($"MoSold"))
//         .show

    spark.stop()
  }
}

object PipelineTest2 {
  def main(args: Array[String]): Unit = {
    val pt2: PipelineTest2  = new PipelineTest2("data/HousePrice/",  null)
    pt2.readData()
  }
}
