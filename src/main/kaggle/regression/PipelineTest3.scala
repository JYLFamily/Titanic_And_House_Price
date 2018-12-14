package main.kaggle.regression

import scala.collection.immutable.Map
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor
import com.microsoft.ml.spark.LightGBMRegressor

class PipelineTest3(private val inputPath: String, private val outputPath: String) {
  private val spark: SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .config("spark.driver.memory", "4G")
    .config("spark.executor.memory", "4G")
    .appName("PipelineTest2")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  private var train: DataFrame = _
  private var test: DataFrame = _
  private var sampleSubmission: DataFrame = _
  private var numericColumns: Array[String] = _
  private var categoricalColumns: Array[String] = _

  private var featureColumns: Array[String] = _
  private var vectorAssembler: VectorAssembler = _
  private var xGBR: XGBoostRegressor = _
  private var lGBR: LightGBMRegressor = _

  private var crossValidator: CrossValidator = _
  private var crossValidarotModel: CrossValidatorModel = _
  private var paramGridBuilder: Array[ParamMap] = _

  def readData(): Unit = {
    this.train = this.spark
      .read
      .option(key = "header", value = true)
      .option(key = "inferSchema", value = true)
      .option(key = "nullValue", value = "")
      .csv(this.inputPath + "train.csv")

    this.test = this.spark
      .read
      .option(key = "header", value = true)
      .option(key = "inferSchema", value = true)
      .option(key = "nullValue", value = "")
      .csv(this.inputPath + "test.csv")
  }

  def prepareData(): Unit = {
    /*
    * Feature Engineering
    * LotArea        地皮面积
    * TotalBsmtSF    地下室面积
    * 1stFlrSF       地上第一层面积
    * 2ndFlrSF       地上第二层面积
    * GrLivArea      地上面积
    * */

    // MSSubClass && MSZoning
    this.train = this.train
      .na
      .replace("MSZoning", Map[String, String]("C (all)" -> "C"))
    this.test = this.test
      .na
      .replace(
        Seq[String]("MSZoning", "BsmtFinSF1", "BsmtFinSF2", "BsmtUnfSF", "TotalBsmtSF", "BsmtFullBath", "BsmtHalfBath", "GarageCars", "GarageArea"),
        Map[String, String]("C (all)" -> "C", "NA" -> null)
      )
    this.test = this.test
      .withColumn("BsmtFinSF1Temp", $"BsmtFinSF1".cast(IntegerType))
      .drop("BsmtFinSF1")
      .withColumnRenamed("BsmtFinSF1Temp", "BsmtFinSF1")
      .withColumn("BsmtFinSF2Temp", $"BsmtFinSF2".cast(IntegerType))
      .drop("BsmtFinSF2")
      .withColumnRenamed("BsmtFinSF2Temp", "BsmtFinSF2")
      .withColumn("BsmtUnfSFTemp", $"BsmtUnfSF".cast(IntegerType))
      .drop("BsmtUnfSF")
      .withColumnRenamed("BsmtUnfSFTemp", "BsmtUnfSF")
      .withColumn("TotalBsmtSFTemp", $"TotalBsmtSF".cast(IntegerType))
      .drop("TotalBsmtSF")
      .withColumnRenamed("TotalBsmtSFTemp", "TotalBsmtSF")
      .withColumn("BsmtFullBathTemp", $"BsmtFullBath".cast(IntegerType))
      .drop("BsmtFullBath")
      .withColumnRenamed("BsmtFullBathTemp", "BsmtFullBath")
      .withColumn("BsmtHalfBathTemp", $"BsmtHalfBath".cast(IntegerType))
      .drop("BsmtHalfBath")
      .withColumnRenamed("BsmtHalfBathTemp", "BsmtHalfBath")
      .withColumn("GarageCarsTemp", $"GarageCars".cast(IntegerType))
      .drop("GarageCars")
      .withColumnRenamed("GarageCarsTemp", "GarageCars")
      .withColumn("GarageAreaTemp", $"GarageArea".cast(IntegerType))
      .drop("GarageArea")
      .withColumnRenamed("GarageAreaTemp", "GarageArea")

    this.train = this.train
      .withColumn("MSSubClassTemp", $"MSSubClass".cast(StringType))
      .drop("MSSubClass")
      .withColumnRenamed("MSSubClassTemp", "MSSubClass")
    this.test = this.test
      .withColumn("MSSubClassTemp", $"MSSubClass".cast(StringType))
      .drop("MSSubClass")
      .withColumnRenamed("MSSubClassTemp", "MSSubClass")

    this.train = this.train
      .withColumn("MSSubClass_MSZoning", concat_ws("_", $"MSSubClass", $"MSZoning"))
      .drop("MSSubClass", "MSZoning")
    this.test = this.test
      .withColumn("MSSubClass_MSZoning", concat_ws("_", $"MSSubClass", $"MSZoning"))
      .drop("MSSubClass", "MSZoning")

    // LotFrontage
    this.train = this.train
      .na
      .replace("LotFrontage", Map[String, String]("NA" -> "-9999"))
      .withColumn("LotFrontageTemp", $"LotFrontage".cast(DoubleType))
      .drop("LotFrontage")
      .withColumnRenamed("LotFrontageTemp", "LotFrontage")
    this.test = this.test
      .na
      .replace("LotFrontage", Map[String, String]("NA" -> "-9999"))
      .withColumn("LotFrontageTemp", $"LotFrontage".cast(DoubleType))
      .drop("LotFrontage")
      .withColumnRenamed("LotFrontageTemp", "LotFrontage")

    // Street && Alley
    this.train = this.train
      .withColumn("Street_Alley", concat_ws("_", $"Street", $"Alley"))
      .drop("Street", "Alley")
    this.test = this.test
      .withColumn("Street_Alley", concat_ws("_", $"Street", $"Alley"))
      .drop("Street", "Alley")

    // LandContour && LandSlope
    this.train = this.train
      .withColumn("LandContour_LandSlope", concat_ws("_", $"LandContour", $"LandSlope"))
      .drop("LandContour", "LandSlope")
    this.test = this.test
      .withColumn("LandContour_LandSlope", concat_ws("_", $"LandContour", $"LandSlope"))
      .drop("LandContour", "LandSlope")

    // LotShape && LotConfig
    this.train = this.train
      .withColumn("LotShape_LotConfig", concat_ws("_", $"LotShape", $"LotConfig"))
      .drop("LotShape", "LotConfig")
    this.test = this.test
      .withColumn("LotShape_LotConfig", concat_ws("_", $"LotShape", $"LotConfig"))
      .drop("LotShape", "LotConfig")

    // Neighborhood && Condition1 && Condition2
    this.train = this.train
      .withColumn("Neighborhood_Condition1_Condition2", concat_ws("_", $"Neighborhood", $"Condition1", $"Condition2"))
      .drop("Neighborhood", "Condition1", "Condition2")
    this.test = this.test
      .withColumn("Neighborhood_Condition1_Condition2", concat_ws("_", $"Neighborhood", $"Condition1", $"Condition2"))
      .drop("Neighborhood", "Condition1", "Condition2")

    // OverallQual && OverallCond
    this.train = this.train
      .withColumn("ADD(OverallQual, OverallCond)", $"OverallQual" + $"OverallCond")
      .drop("OverallQual", "OverallCond")
    this.test = this.test
      .withColumn("ADD(OverallQual, OverallCond)", $"OverallQual" + $"OverallCond")
      .drop("OverallQual", "OverallCond")

    // YearBuilt && YearRemodAdd
    this.train = this.train
      .withColumn(
        "SUB(YearRemodAdd, YearBuilt)",
        when($"YearRemodAdd" - $"YearBuilt" > lit(0), $"YearRemodAdd" - $"YearBuilt").otherwise(lit(0)))
      .withColumn("DateDiff(Now, YearRemodAdd)", year(current_date()).cast(DoubleType) - $"YearRemodAdd".cast(DoubleType))
      .withColumn("DateDiff(Now, YearBuilt)", year(current_date()).cast(DoubleType) - $"YearBuilt".cast(DoubleType))
    this.test = this.test
      .withColumn(
        "SUB(YearRemodAdd, YearBuilt)",
        when($"YearRemodAdd" - $"YearBuilt" > lit(0), $"YearRemodAdd" - $"YearBuilt").otherwise(lit(0)))
      .withColumn("DateDiff(Now, YearRemodAdd)", year(current_date()).cast(DoubleType) - $"YearRemodAdd".cast(DoubleType))
      .withColumn("DateDiff(Now, YearBuilt)", year(current_date()).cast(DoubleType) - $"YearBuilt".cast(DoubleType))

    // RoofStyle && RoofMatl
    this.train = this.train
      .withColumn("RoofStyle_RoofMatl", concat_ws("_", $"RoofStyle", $"RoofMatl"))
      .drop("RoofStyle", "RoofMatl")
    this.test = this.test
      .withColumn("RoofStyle_RoofMatl", concat_ws("_", $"RoofStyle", $"RoofMatl"))
      .drop("RoofStyle", "RoofMatl")

    // Exterior1st && Exterior2nd
    this.train = this.train
      .withColumn("Exterior1st_Exterior2nd", concat_ws("_", $"Exterior1st", $"Exterior2nd"))
      .drop("Exterior1st", "Exterior2nd")
    this.test = this.test
      .withColumn("Exterior1st_Exterior2nd", concat_ws("_", $"Exterior1st", $"Exterior2nd"))
      .drop("Exterior1st", "Exterior2nd")

    // MasVnrType
    this.train = this.train
      .na
      .replace("MasVnrType", Map[String, String]("NA" -> null))
    this.test = this.test
      .na
      .replace("MasVnrType", Map[String, String]("NA" -> null))

    // MasVnrArea
    this.train = this.train
      .na
      .replace("MasVnrArea", Map[String, String]("NA" -> "-9999"))
      .withColumn("MasVnrAreaTemp", $"MasVnrArea".cast(DoubleType))
      .drop("MasVnrArea")
      .withColumnRenamed("MasVnrAreaTemp", "MasVnrArea")
    this.test = this.test
      .na
      .replace("MasVnrArea", Map[String, String]("NA" -> "-9999"))
      .withColumn("MasVnrAreaTemp", $"MasVnrArea".cast(DoubleType))
      .drop("MasVnrArea")
      .withColumnRenamed("MasVnrAreaTemp", "MasVnrArea")

    this.train = this.train
      .withColumn("DIVIDE(MasVnrArea, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"MasVnrArea" / $"LotArea"))
      .withColumn("DIVIDE(MasVnrArea, ADD(TotalBsmtSF, GrLivArea))", when(($"TotalBsmtSF" + $"GrLivArea") === lit(0), -9999).otherwise($"MasVnrArea" / ($"TotalBsmtSF" + $"GrLivArea")))

    this.test = this.test
      .withColumn("DIVIDE(MasVnrArea, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"MasVnrArea" / $"LotArea"))
      .withColumn("DIVIDE(MasVnrArea, ADD(TotalBsmtSF, GrLivArea))", when(($"TotalBsmtSF" + $"GrLivArea") === lit(0), -9999).otherwise($"MasVnrArea" / ($"TotalBsmtSF" + $"GrLivArea")))

    // ExterQual && ExterCond
    this.train = this.train
      .withColumn("ExterQual_ExterCond", concat_ws("_", $"ExterQual", $"ExterCond"))
      .drop("ExterQual", "ExterCond")
    this.test = this.test
      .withColumn("ExterQual_ExterCond", concat_ws("_", $"ExterQual", $"ExterCond"))
      .drop("ExterQual", "ExterCond")

    // BsmtQual && BsmtCond && BsmtExposure && BsmtFinType1 && BsmtFinType2
    this.train = this.train
      .withColumn("BsmtQual_BsmtCond_BsmtExposure_BsmtFinType1_BsmtFinType2", concat_ws("_", $"BsmtQual", $"BsmtCond", $"BsmtExposure", $"BsmtFinType1", $"BsmtFinType2"))
      .drop("BsmtQual", "BsmtCond", "BsmtExposure", "BsmtFinType1", "BsmtFinType2")
    this.test = this.test
      .withColumn("BsmtQual_BsmtCond_BsmtExposure_BsmtFinType1_BsmtFinType2", concat_ws("_", $"BsmtQual", $"BsmtCond", $"BsmtExposure", $"BsmtFinType1", $"BsmtFinType2"))
      .drop("BsmtQual", "BsmtCond", "BsmtExposure", "BsmtFinType1", "BsmtFinType2")

    // BsmtFinSF1 && BsmtFinSF2
    this.train = this.train
      .withColumn("ADD(BsmtFinSF1, BsmtFinSF2)", $"BsmtFinSF2" + $"BsmtFinSF2")
      .withColumn("DIVIDE(BsmtFinSF1, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"BsmtFinSF1" / $"LotArea"))
      .withColumn("DIVIDE(BsmtFinSF2, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"BsmtFinSF2" / $"LotArea"))
      .withColumn("DIVIDE(BsmtFinSF1, TotalBsmtSF)", when($"TotalBsmtSF" === lit(0), -9999).otherwise($"BsmtFinSF1" / $"TotalBsmtSF"))
      .withColumn("DIVIDE(BsmtFinSF2, TotalBsmtSF)", when($"TotalBsmtSF" === lit(0), -9999).otherwise($"BsmtFinSF2" / $"TotalBsmtSF"))
      .withColumn("DIVIDE(BsmtFinSF1, ADD(TotalBsmtSF, GrLivArea))", when(($"TotalBsmtSF" + $"GrLivArea") === lit(0), -9999).otherwise($"BsmtFinSF1" / ($"TotalBsmtSF" + $"GrLivArea")))
      .withColumn("DIVIDE(BsmtFinSF2, ADD(TotalBsmtSF, GrLivArea))", when(($"TotalBsmtSF" + $"GrLivArea") === lit(0), -9999).otherwise($"BsmtFinSF2" / ($"TotalBsmtSF" + $"GrLivArea")))
      .withColumn("DIVIDE(ADD(BsmtFinSF1, BsmtFinSF2), LotArea)", when($"LotArea" === lit(0), -9999).otherwise(($"BsmtFinSF1" + $"BsmtFinSF2") / $"LotArea"))
      .withColumn("DIVIDE(ADD(BsmtFinSF1, BsmtFinSF2), TotalBsmtSF)", when($"TotalBsmtSF" === lit(0), -9999).otherwise(($"BsmtFinSF1" + $"BsmtFinSF2") / $"TotalBsmtSF"))
      .withColumn("DIVIDE(ADD(BsmtFinSF1, BsmtFinSF2), ADD(TotalBsmtSF, GrLivArea))", when(($"TotalBsmtSF" + $"GrLivArea") === lit(0), -9999).otherwise(($"BsmtFinSF1" + $"BsmtFinSF2") / ($"TotalBsmtSF" + $"GrLivArea")))
    this.test = this.test
      .withColumn("ADD(BsmtFinSF1, BsmtFinSF2)", $"BsmtFinSF2" + $"BsmtFinSF2")
      .withColumn("DIVIDE(BsmtFinSF1, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"BsmtFinSF1" / $"LotArea"))
      .withColumn("DIVIDE(BsmtFinSF2, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"BsmtFinSF2" / $"LotArea"))
      .withColumn("DIVIDE(BsmtFinSF1, TotalBsmtSF)", when($"TotalBsmtSF" === lit(0), -9999).otherwise($"BsmtFinSF1" / $"TotalBsmtSF"))
      .withColumn("DIVIDE(BsmtFinSF2, TotalBsmtSF)", when($"TotalBsmtSF" === lit(0), -9999).otherwise($"BsmtFinSF2" / $"TotalBsmtSF"))
      .withColumn("DIVIDE(BsmtFinSF1, ADD(TotalBsmtSF, GrLivArea))", when(($"TotalBsmtSF" + $"GrLivArea") === lit(0), -9999).otherwise($"BsmtFinSF1" / ($"TotalBsmtSF" + $"GrLivArea")))
      .withColumn("DIVIDE(BsmtFinSF2, ADD(TotalBsmtSF, GrLivArea))", when(($"TotalBsmtSF" + $"GrLivArea") === lit(0), -9999).otherwise($"BsmtFinSF2" / ($"TotalBsmtSF" + $"GrLivArea")))
      .withColumn("DIVIDE(ADD(BsmtFinSF1, BsmtFinSF2), LotArea)", when($"LotArea" === lit(0), -9999).otherwise(($"BsmtFinSF1" + $"BsmtFinSF2") / $"LotArea"))
      .withColumn("DIVIDE(ADD(BsmtFinSF1, BsmtFinSF2), TotalBsmtSF)", when($"TotalBsmtSF" === lit(0), -9999).otherwise(($"BsmtFinSF1" + $"BsmtFinSF2") / $"TotalBsmtSF"))
      .withColumn("DIVIDE(ADD(BsmtFinSF1, BsmtFinSF2), ADD(TotalBsmtSF, GrLivArea))", when(($"TotalBsmtSF" + $"GrLivArea") === lit(0), -9999).otherwise(($"BsmtFinSF1" + $"BsmtFinSF2") / ($"TotalBsmtSF" + $"GrLivArea")))

    // BsmtUnfSF && TotalBsmtSF
    this.train = this.train
      .withColumn("DIVIDE(BsmtUnfSF, TotalBsmtSF)", when($"TotalBsmtSF" === lit(0), -9999).otherwise($"BsmtUnfSF" / $"TotalBsmtSF"))
      .withColumn("DIVIDE(BsmtUnfSF, ADD(BsmtFinSF1, BsmtFinSF2))", when($"BsmtFinSF1" + $"BsmtFinSF2" === lit(0), -9999).otherwise($"BsmtUnfSF" / ($"BsmtFinSF1" + $"BsmtFinSF2")))
      .withColumn("DIVIDE(BsmtUnfSF, ADD(TotalBsmtSF, GrLivArea))", when($"TotalBsmtSF" + $"GrLivArea" === lit(0), -9999).otherwise($"BsmtUnfSF" / ($"TotalBsmtSF" + $"GrLivArea")))
      .withColumn("DIVIDE(TotalBsmtSF, ADD(TotalBsmtSF, GrLivArea))", when($"TotalBsmtSF" + $"GrLivArea" === lit(0), -9999).otherwise($"TotalBsmtSF" / ($"TotalBsmtSF" + $"GrLivArea")))
    this.test = this.test
      .withColumn("DIVIDE(BsmtUnfSF, TotalBsmtSF)", when($"TotalBsmtSF" === lit(0), -9999).otherwise($"BsmtUnfSF" / $"TotalBsmtSF"))
      .withColumn("DIVIDE(BsmtUnfSF, ADD(BsmtFinSF1, BsmtFinSF2))", when($"BsmtFinSF1" + $"BsmtFinSF2" === lit(0), -9999).otherwise($"BsmtUnfSF" / ($"BsmtFinSF1" + $"BsmtFinSF2")))
      .withColumn("DIVIDE(BsmtUnfSF, ADD(TotalBsmtSF, GrLivArea))", when($"TotalBsmtSF" + $"GrLivArea" === lit(0), -9999).otherwise($"BsmtUnfSF" / ($"TotalBsmtSF" + $"GrLivArea")))
      .withColumn("DIVIDE(TotalBsmtSF, ADD(TotalBsmtSF, GrLivArea))", when($"TotalBsmtSF" + $"GrLivArea" === lit(0), -9999).otherwise($"TotalBsmtSF" / ($"TotalBsmtSF" + $"GrLivArea")))

    // Heating && HeatingQC
    this.train = this.train
      .withColumn("Heating_HeatingQC", concat_ws("_", $"Heating", $"HeatingQC"))
      .drop("Heating", "HeatingQC")
    this.test = this.test
      .withColumn("Heating_HeatingQC", concat_ws("_", $"Heating", $"HeatingQC"))
      .drop("Heating", "HeatingQC")

    // 1stFlrSF && 2ndFlrSF && LowQualFinSF & GrLivArea
    this.train = this.train
      .withColumn("ADD(1stFlrSF, 2ndFlrSF)", $"1stFlrSF" + $"2ndFlrSF")
      .withColumn("DIVIDE(2ndFlrSF, 1stFlrSF)", when($"2ndFlrSF" === lit(0), -9999).otherwise($"2ndFlrSF" / $"1stFlrSF"))
      .withColumn("DIVIDE(1stFlrSF, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"1stFlrSF" / $"LotArea"))
      .withColumn("DIVIDE(2ndFlrSF, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"2ndFlrSF" / $"LotArea"))
      .withColumn("DIVIDE(ADD(1stFlrSF, 2ndFlrSF), LotArea)", when($"LotArea" === lit(0), -9999).otherwise(($"1stFlrSF" + $"2ndFlrSF") / $"LotArea"))
      .withColumn("DIVIDE(LowQualFinSF, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"LowQualFinSF" / $"LotArea"))
      .withColumn("DIVIDE(GrLivArea, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"GrLivArea" / $"LotArea"))
      .withColumn("DIVIDE(1stFlrSF, ADD(TotalBsmtSF, GrLivArea))", when($"TotalBsmtSF" + $"GrLivArea" === lit(0), -9999).otherwise($"1stFlrSF" / ($"TotalBsmtSF" + $"GrLivArea")))
      .withColumn("DIVIDE(2ndFlrSF, ADD(TotalBsmtSF, GrLivArea))", when($"TotalBsmtSF" + $"GrLivArea" === lit(0), -9999).otherwise($"2ndFlrSF" / ($"TotalBsmtSF" + $"GrLivArea")))
      .withColumn("DIVIDE(GrLivArea, ADD(TotalBsmtSF, GrLivArea))", when($"TotalBsmtSF" + $"GrLivArea" === lit(0), -9999).otherwise($"GrLivArea" / ($"TotalBsmtSF" + $"GrLivArea")))
    this.test = this.test
      .withColumn("ADD(1stFlrSF, 2ndFlrSF)", $"1stFlrSF" + $"2ndFlrSF")
      .withColumn("DIVIDE(2ndFlrSF, 1stFlrSF)", when($"2ndFlrSF" === lit(0), -9999).otherwise($"2ndFlrSF" / $"1stFlrSF"))
      .withColumn("DIVIDE(1stFlrSF, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"1stFlrSF" / $"LotArea"))
      .withColumn("DIVIDE(2ndFlrSF, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"2ndFlrSF" / $"LotArea"))
      .withColumn("DIVIDE(ADD(1stFlrSF, 2ndFlrSF), LotArea)", when($"LotArea" === lit(0), -9999).otherwise(($"1stFlrSF" + $"2ndFlrSF") / $"LotArea"))
      .withColumn("DIVIDE(LowQualFinSF, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"LowQualFinSF" / $"LotArea"))
      .withColumn("DIVIDE(GrLivArea, LotArea)", when($"LotArea" === lit(0), -9999).otherwise($"GrLivArea" / $"LotArea"))
      .withColumn("DIVIDE(1stFlrSF, ADD(TotalBsmtSF, GrLivArea))", when($"TotalBsmtSF" + $"GrLivArea" === lit(0), -9999).otherwise($"1stFlrSF" / ($"TotalBsmtSF" + $"GrLivArea")))
      .withColumn("DIVIDE(2ndFlrSF, ADD(TotalBsmtSF, GrLivArea))", when($"TotalBsmtSF" + $"GrLivArea" === lit(0), -9999).otherwise($"2ndFlrSF" / ($"TotalBsmtSF" + $"GrLivArea")))
      .withColumn("DIVIDE(GrLivArea, ADD(TotalBsmtSF, GrLivArea))", when($"TotalBsmtSF" + $"GrLivArea" === lit(0), -9999).otherwise($"GrLivArea" / ($"TotalBsmtSF" + $"GrLivArea")))

    // BedroomAbvGr && KitchenAbvGr && KitchenQual && TotRmsAbvGrd
    this.train = this.train
      .withColumn("DIVIDE(BedroomAbvGr, TotRmsAbvGrd)", when($"TotRmsAbvGrd" === lit(0), -9999).otherwise($"BedroomAbvGr" / $"TotRmsAbvGrd"))
      .withColumn("DIVIDE(KitchenAbvGr, TotRmsAbvGrd)", when($"TotRmsAbvGrd" === lit(0), -9999).otherwise($"KitchenAbvGr" / $"TotRmsAbvGrd"))
      .withColumn("KitchenAbvGr_KitchenQual", concat_ws("_", $"KitchenAbvGr".cast(StringType), $"KitchenQual"))
    this.test = this.test
      .withColumn("DIVIDE(BedroomAbvGr, TotRmsAbvGrd)", when($"TotRmsAbvGrd" === lit(0), -9999).otherwise($"BedroomAbvGr" / $"TotRmsAbvGrd"))
      .withColumn("DIVIDE(KitchenAbvGr, TotRmsAbvGrd)", when($"TotRmsAbvGrd" === lit(0), -9999).otherwise($"KitchenAbvGr" / $"TotRmsAbvGrd"))
      .withColumn("KitchenAbvGr_KitchenQual", concat_ws("_", $"KitchenAbvGr".cast(StringType), $"KitchenQual"))

    // Fireplaces && FireplaceQu
    this.train = this.train
      .withColumn("Fireplaces_FireplaceQu", concat_ws("_", $"Fireplaces".cast(StringType), $"FireplaceQu"))
    this.test = this.test
      .withColumn("Fireplaces_FireplaceQu", concat_ws("_", $"Fireplaces".cast(StringType), $"FireplaceQu"))

    // GarageType && GarageYrBlt && GarageFinish && GarageCars && GarageArea && GarageQual && GarageCond
    this.train = this.train
      .withColumn("GarageType_GarageFinish_GarageQual_GarageCond", concat_ws("_", $"GarageType", $"GarageFinish", $"GarageQual", $"GarageCond"))
      .drop("GarageType", "GarageFinish", "GarageQual", "GarageCond")
    this.test = this.test
      .withColumn("GarageType_GarageFinish_GarageQual_GarageCond", concat_ws("_", $"GarageType", $"GarageFinish", $"GarageQual", $"GarageCond"))
      .drop("GarageType", "GarageFinish", "GarageQual", "GarageCond")

    this.train = this.train
      .na
      .replace("GarageYrBlt", Map[String, String]("NA" -> "-9999"))
      .withColumn("GarageYrBltTemp", $"GarageYrBlt".cast(DoubleType))
      .drop("GarageYrBlt")
      .withColumnRenamed("GarageYrBltTemp", "GarageYrBlt")
      .withColumn("DateDiff(Now, GarageYrBlt)", when($"GarageYrBlt" === lit(-9999), -9999).otherwise(year(current_date()).cast(DoubleType) - $"GarageYrBlt"))
      .drop("GarageYrBlt")
    this.test = this.test
      .na
      .replace("GarageYrBlt", Map[String, String]("NA" -> "-9999"))
      .withColumn("GarageYrBltTemp", $"GarageYrBlt".cast(DoubleType))
      .drop("GarageYrBlt")
      .withColumnRenamed("GarageYrBltTemp", "GarageYrBlt")
      .withColumn("DateDiff(Now, GarageYrBlt)", when($"GarageYrBlt" === lit(-9999), -9999).otherwise(year(current_date()).cast(DoubleType) - $"GarageYrBlt"))
      .drop("GarageYrBlt")

    this.train = this.train
      .withColumn("DIVIDE(GarageArea, GarageCars)", when($"GarageCars" === lit(0), -9999).otherwise($"GarageArea" / $"GarageCars"))
    this.test = this.test
      .withColumn("DIVIDE(GarageArea, GarageCars)", when($"GarageCars" === lit(0), -9999).otherwise($"GarageArea" / $"GarageCars"))

    // OpenPorchSF && EnclosedPorch && 3SsnPorch && ScreenPorch
    this.train = this.train
      .withColumn("ADD(OpenPorchSF, EnclosedPorch, 3SsnPorch, ScreenPorch)", $"OpenPorchSF" + $"EnclosedPorch" + $"3SsnPorch" + $"ScreenPorch")
    this.test = this.test
      .withColumn("ADD(OpenPorchSF, EnclosedPorch, 3SsnPorch, ScreenPorch)", $"OpenPorchSF" + $"EnclosedPorch" + $"3SsnPorch" + $"ScreenPorch")

    // YrSold && MoSold
    this.train = this.train
      .withColumn("YrSoldMoSold", concat_ws("-",$"YrSold", when(length($"MoSold") === 1, concat(lit("0"), $"MoSold")).otherwise($"MoSold"), lit("01")).cast(DateType))
      .withColumn("DateDiff(Now, YrSoldMoSold)", datediff(current_date(), $"YrSoldMoSold"))
      .withColumn("DateDiff(YrSoldMoSold, YearBuilt)", datediff($"YrSoldMoSold", concat_ws("-", $"YearBuilt", lit("01"), lit("01")).cast(DateType)))
      .withColumn("DateDiff(YrSoldMoSold, YearRemodAdd)", datediff($"YrSoldMoSold", concat_ws("-", $"YearRemodAdd", lit("01"), lit("01")).cast(DateType)))
      .drop("YrSoldMoSold", "YearBuilt", "YearRemodAdd", "YrSold", "MoSold")
    this.test = this.test
      .withColumn("YrSoldMoSold", concat_ws("-",$"YrSold", when(length($"MoSold") === 1, concat(lit("0"), $"MoSold")).otherwise($"MoSold"), lit("01")).cast(DateType))
      .withColumn("DateDiff(Now, YrSoldMoSold)", datediff(current_date(), $"YrSoldMoSold"))
      .withColumn("DateDiff(YrSoldMoSold, YearBuilt)", datediff($"YrSoldMoSold", concat_ws("-", $"YearBuilt", lit("01"), lit("01")).cast(DateType)))
      .withColumn("DateDiff(YrSoldMoSold, YearRemodAdd)", datediff($"YrSoldMoSold", concat_ws("-", $"YearRemodAdd", lit("01"), lit("01")).cast(DateType)))
      .drop("YrSoldMoSold", "YearBuilt", "YearRemodAdd", "YrSold", "MoSold")

    // SaleType && SaleCondition
    this.train = this.train
      .withColumn("SaleType_SaleCondition", concat_ws("_", $"SaleType", $"SaleCondition"))
      .drop("SaleType", "SaleCondition")
    this.test = this.test
      .withColumn("SaleType_SaleCondition", concat_ws("_", $"SaleType", $"SaleCondition"))
      .drop("SaleType", "SaleCondition")

    this.numericColumns =
      for (tuple <- this.train.dtypes if ! tuple._1.equals("SalePrice") && ! tuple._1.equals("Id") && ! tuple._2.equals("StringType")) yield tuple._1
    this.categoricalColumns =
      for (tuple <- this.train.dtypes if ! tuple._1.equals("SalePrice") && ! tuple._1.equals("Id") && tuple._2.equals("StringType")) yield tuple._1

    // numeric feature
    this.train = this.train
      .na
      .fill(-9999, numericColumns)
    this.test = this.test
      .na
      .fill(-9999, numericColumns)

    // encoder categorical feature
    for (col <- categoricalColumns) {
      val stringIndexer: StringIndexer = new StringIndexer()
        .setInputCol(col)
        .setOutputCol(col + "_Index")
        .setHandleInvalid("keep")
      val stringIndexerModel: StringIndexerModel = stringIndexer.fit(this.train)
      this.train = stringIndexerModel.transform(this.train)
      this.test = stringIndexerModel.transform(this.test)
      this.train = this.train.drop(col)
      this.test = this.test.drop(col)
    }

    /*
     * Target Engineering
     * */
    this.train = this.train
      .withColumn("label", log1p($"SalePrice" / $"GrLivArea"))
  }

  def modelFitPredict(): Unit = {
    this.featureColumns =
      for (col <- this.train.columns if ! col.equals("SalePrice") && ! col.equals("Id") && ! col.equals("label")) yield col

    this.vectorAssembler = new VectorAssembler()
      .setInputCols(this.featureColumns)
      .setOutputCol("features")
    this.train = vectorAssembler.transform(this.train)
    this.test = vectorAssembler.transform(this.test)

//    this.xGBR = new XGBoostRegressor()
//      .setFeaturesCol(this.vectorAssembler.getOutputCol)
//      .setLabelCol("label")
//      .setPredictionCol("prediction")
//      .setMaxBins(9999)
//      .setMissing(-9999)
//      .setSeed(7)

    this.lGBR = new LightGBMRegressor()
      .setFeaturesCol(this.vectorAssembler.getOutputCol)
      .setLabelCol("label")
      .setPredictionCol("prediction")

//    this.paramGridBuilder = new ParamGridBuilder()
//      .addGrid(this.lightGBM.learningRate, Array(0.05, 0.1, 0.15))
//      .addGrid(this.lightGBM.numIterations, Array(15, 20, 25, 30))
//      .addGrid(this.lightGBM.maxDepth, Array(5, 6, 7, 8))
//      .addGrid(this.lightGBM.baggingFraction, Array(0.65, 0.75, 0.85))
//      .addGrid(this.lightGBM.featureFraction, Array(0.65, 0.75, 0.85))
//      .build()
//
//    this.crossValidator = new CrossValidator()
//      .setEstimator(this.lightGBM)
//      .setEvaluator(new RegressionEvaluator().setMetricName("rmse"))
//      .setEstimatorParamMaps(this.paramGridBuilder)
//      .setNumFolds(3)
//
//    this.crossValidarotModel = this.crossValidator.fit(this.train)
//    this.test = this.crossValidarotModel.transform(this.test)
//      .withColumn("SalePrice", expm1($"prediction") * $"GrLivArea")

//    this.test = this.xGBR
//      .fit(this.train)
//      .transform(this.test)

    this.test = this.lGBR
      .fit(this.train)
      .transform(this.test)

    this.sampleSubmission = this.test.select($"Id", $"SalePrice")
    this.sampleSubmission
      .repartition(1)
      .write
      .option(key = "header", value = true)
      .csv("C:\\Users\\jiangyilan\\Desktop\\sample_submission")

    this.spark.stop()
  }
}

object PipelineTest3 {
  def main(args: Array[String]): Unit = {
    val pt3: PipelineTest3  = new PipelineTest3("data/HousePrice/",  null)
    pt3.readData()
    pt3.prepareData()
    pt3.modelFitPredict()
  }
}
