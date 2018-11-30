package main.kaggle.regression

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressor, GBTRegressionModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PipelineTest2(private val inputPath: String, private val outputPath: String) {
  private val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.driver.memory", "4G")
      .config("spark.executor.memory", "4G")
      .appName("PipelineTest2")
      .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  private var train: DataFrame = _
  private var test: DataFrame = _
  private var numericColumns: Array[String] = _
  private var categoricalColumns: Array[String] = _
  private var vectorAssembler: VectorAssembler = _

  def readData(): Unit = {
    this.train = this.spark
      .read
      .option(key = "header", value = true)
      .option(key = "inferSchema", value = true)
      .csv(this.inputPath + "train.csv")
      .repartition(1)
      .cache()

    this.test = this.spark
      .read
      .option(key = "header", value = true)
      .option(key = "inferSchema", value = true)
      .csv(this.inputPath + "test.csv")
      .repartition(1)
      .cache()
  }

  def prepareData(): Unit = {
    this.train = this.train
      .drop("Id")
    this.test = this.test
      .drop("Id")

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
      .replace("MSZoning", Map[String, String]("C (all)" -> "C"))

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

    numericColumns = for (tuple <- this.train.dtypes if ! tuple._1.equals("SalePrice") && ! tuple._2.equals("StringType")) yield tuple._1
    categoricalColumns = for (tuple <- this.train.dtypes if ! tuple._1.equals("SalePrice") && tuple._2.equals("StringType")) yield tuple._1

    // numeric feature
    this.train = this.train
      .na
      .fill(-9999, numericColumns)
    this.test = this.test
      .na
      .fill(-9999, numericColumns)

    // encoder categorical feature
    val prior = this.train.select(mean($"SalePrice" / ($"TotalBsmtSF" + $"GrLivArea")) as "prior")
      .map(row => row.getDouble(0))
      .first()
    for (col <- categoricalColumns) {
      println(col)
      val stringIndexer: StringIndexer = new StringIndexer()
        .setInputCol(col)
        .setOutputCol(col + "_Index")
        .setHandleInvalid("keep")
      val stringIndexerModel: StringIndexerModel = stringIndexer.fit(this.train)
      this.train = stringIndexerModel.transform(this.train)
      this.test = stringIndexerModel.transform(this.test)

      val coeff = this.train
        .groupBy(col + "_Index")
        .agg(count(col + "_Index") as "count")
        .select(this.train(col + "_Index"), (lit(1) / (lit(1) + exp($"count" - lit(1)))) as "coeff")
      val posterior = this.train
        .groupBy(col + "_Index")
        .agg(mean($"SalePrice" / ($"TotalBsmtSF" + $"GrLivArea")) as "posterior")
      val coeffPriorPosterior = coeff
        .join(posterior, Seq[String](col + "_Index"))
        .withColumn(col + "_Target", $"coeff" * lit(prior) + (lit(1) - $"coeff") * $"posterior")
        .select(col + "_Index", col + "_Target")

      this.train = this.train
        .join(coeffPriorPosterior, Seq[String](col + "_Index"))
      this.test = this.test
        .join(coeffPriorPosterior, Seq[String](col + "_Index"))
    }

    categoricalColumns = for (tuple <- this.train.dtypes if tuple._1.endsWith("_Target")) yield tuple._1
    this.train = this.train
      .na
      .fill(prior, categoricalColumns)
    this.test = this.test
      .na
      .fill(prior, categoricalColumns)

    /*
     * Target Engineering
     * */
    this.train = this.train
        .withColumn("label", log1p($"SalePrice" / ($"TotalBsmtSF" + $"GrLivArea")))
    this.test = this.test
        .withColumn("label", log1p($"SalePrice" / ($"TotalBsmtSF" + $"GrLivArea")))

    this.train.show
    this.test.show

    spark.stop()
  }

  def modelFitPredict(): Unit = {
    numericColumns = for (tuple <- this.train.dtypes if  ! tuple._1.equals("label") && ! tuple._1.equals("SalePrice") && ! tuple._2.equals("StringType")) yield tuple._1
    categoricalColumns = for (tuple <- this.train.dtypes if tuple._1.endsWith("_Index") || tuple._1.endsWith("_Target")) yield tuple._1

    vectorAssembler = new VectorAssembler()
      .setInputCols(numericColumns)
      .setOutputCol("numeric_feature")
    this.train = vectorAssembler.transform(this.train)
    this.test = vectorAssembler.transform(this.test)
  }
}

object PipelineTest2 {
  def main(args: Array[String]): Unit = {
    val pt2: PipelineTest2  = new PipelineTest2("data/HousePrice/",  null)
    pt2.readData()
    pt2.prepareData()
//    pt2.modelFitPredict()
  }
}
