package main.kaggle.regression

import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}

class PipelineTest1(private val inputPath: String, private val outputPath: String) {
  private val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("PipelineTest")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  private var train: DataFrame = _
  private var test: DataFrame = _
  private var featureColumns: Array[String] = _

  private val numericColumns: collection.mutable.Buffer[String] = new collection.mutable.ListBuffer[String]()
  private val categoricalColumns: collection.mutable.Buffer[String] = new collection.mutable.ListBuffer[String]()

  private var assembler: VectorAssembler = _
  private var standardScale: StandardScaler = _
  private var labelEncoder: StringIndexer = _
  private var oneHotEncoder: OneHotEncoder = _

  // model fit predict
  private var gBTRegressor: GBTRegressor = _
  private var paramGrid: Array[ParamMap] = _
  private var crossValidator: CrossValidator = _

  // data write
  private var sampleSubmission: DataFrame = _

  def dataRead(): Unit = {
    this.train = this.spark.read
      .option(key = "header", value = true)
      .option(key = "nullValue", value = "NA")
      .option(key = "inferSchema", value = true)
      .csv(this.inputPath + "train.csv")

    this.test = this.spark.read
      .option(key = "header", value = true)
      .option(key = "nullValue", value = "NA")
      .option(key = "inferSchema", value = true)
      .csv(this.inputPath + "test.csv")
  }

  def dataPrepare(): Unit = {
    this.train = this.train.na.replace(this.train.columns, Map[String, String]("None" -> null))
    this.test = this.test.na.replace(this.test.columns, Map[String, String]("None" -> null))

    this.featureColumns = this.train.columns.filter(columns => columns != "SalePrice")
    this.test = this.test.select(this.featureColumns.toList.head, this.featureColumns.toList.tail: _ *)

    this.train = this.train.withColumn("MSSubClassTmp", this.train("MSSubClass").cast(StringType))
      .drop("MSSubClass")
      .withColumnRenamed("MSSubClassTmp", "MSSubClass")
      .withColumn("OverallCondTmp", this.train("OverallCond").cast(StringType))
      .drop("OverallCond")
      .withColumnRenamed("OverallCondTmp", "OverallCond")
      .withColumn("YrSoldTmp", this.train("YrSold").cast(StringType))
      .drop("YrSold")
      .withColumnRenamed("YrSoldTmp", "YrSold")
      .withColumn("MoSoldTmp", this.train("MoSold").cast(StringType))
      .drop("MoSold")
      .withColumnRenamed("MoSoldTmp", "MoSold")

    this.test = this.test.withColumn("MSSubClassTmp", this.test("MSSubClass").cast(StringType))
      .drop("MSSubClass")
      .withColumnRenamed("MSSubClassTmp", "MSSubClass")
      .withColumn("OverallCondTmp", this.test("OverallCond").cast(StringType))
      .drop("OverallCond")
      .withColumnRenamed("OverallCondTmp", "OverallCond")
      .withColumn("YrSoldTmp", this.test("YrSold").cast(StringType))
      .drop("YrSold")
      .withColumnRenamed("YrSoldTmp", "YrSold")
      .withColumn("MoSoldTmp", this.test("MoSold").cast(StringType))
      .drop("MoSold")
      .withColumnRenamed("MoSoldTmp", "MoSold")

    for (tuple <- this.train.dtypes) {
      if (! tuple._1.equals("SalePrice")) {
        if (tuple._2.equals("StringType")) {
          this.categoricalColumns.append(tuple._1)
        } else {
          if (! tuple._2.equals("Id")) {
            this.numericColumns.append(tuple._1)
          }
        }
      }
    }

    // numeric feature
    this.assembler = new VectorAssembler()
      .setInputCols(this.numericColumns.toArray)
      .setOutputCol("numeric_feature")

    this.train = assembler.transform(this.train.na.fill(0, this.numericColumns.toArray))
    this.test = assembler.transform(this.test.na.fill(0, this.numericColumns.toArray))

    this.standardScale = new StandardScaler().setInputCol("numeric_feature")
      .setOutputCol("scaled_numeric_feature")
      .setWithStd(true)
      .setWithMean(true)

    val standardScaleModel: StandardScalerModel  = this.standardScale.fit(this.train)
    this.train = standardScaleModel.transform(this.train)
    this.test = standardScaleModel.transform(this.test)

    // categorical feature
    for (col <- this.categoricalColumns) {
      val numUnique = this.train.select(col).distinct()
      if (numUnique.count().equals(1)) {
        this.train = this.train.drop(col)
        this.test = this.test.drop(col)
      } else {
        if (this.train.select(col).filter(this.train(col).isNull).count != 0) {
          this.train = this.train.na.fill("missing", Array(col))
          val mode = this.train.select(col)
            .groupBy(col)
            .agg(count(col) as "count")
            .sort(desc("count"))
            .map(row => row.getString(0))
            .first()
          this.test = this.test.na.fill(mode, Array(col))

          val categories = this.test.select(col)
            .distinct()
            .except(this.train.select(col).distinct())
            .map(row => row.getString(0))
            .collect()

          val categoriesToMode = collection.mutable.Map[String, String]()
          for (category <- categories) {
            categoriesToMode += (category -> mode)
          }
          this.test = this.test.na.replace(col, categoriesToMode.toMap)

        } else {
          val mode = this.train.select(col)
            .groupBy(col)
            .agg(count(col) as "count")
            .sort(desc("count"))
            .map(row => row.getString(0))
            .first()
          this.test = this.test.na.fill(mode, Array(col))

          val categories = this.test.select(col)
            .distinct()
            .except(this.train.select(col).distinct())
            .map(row => row.getString(0))
            .collect()

          val categoriesToMode = collection.mutable.Map[String, String]()
          for (category <- categories) {
            categoriesToMode += (category -> mode)
          }
          this.test = this.test.na.replace(col, categoriesToMode.toMap)
        }
      }
    }

    for (col <- this.categoricalColumns) {
      this.labelEncoder = new StringIndexer().setInputCol(col)
        .setOutputCol(col + "_labeled")
      val labelEncoderModel: StringIndexerModel = this.labelEncoder.fit(this.train)
      this.train = labelEncoderModel.transform(this.train)
      this.test = labelEncoderModel.transform(this.test)
    }

    this.assembler = new VectorAssembler()
        .setInputCols(this.train.columns.filter(column => column.endsWith("_labeled")))
        .setOutputCol("categorical_columns")
    this.train = assembler.transform(this.train)
    this.test = assembler.transform(this.test)

    this.oneHotEncoder = new OneHotEncoder()
      .setInputCol(this.labelEncoder.getOutputCol)
      .setOutputCol("one_hot_categorical_feature")
    this.train = this.oneHotEncoder.transform(this.train)
    this.test = this.oneHotEncoder.transform(this.test)

    // numeric + categorical feature
    this.assembler = new VectorAssembler()
      .setInputCols(Array("scaled_numeric_feature", "one_hot_categorical_feature"))
      .setOutputCol("features")
    this.train = this.assembler.transform(this.train)
    this.test = this.assembler.transform(this.test)

    // label log transform
    this.train = this.train.withColumn("label", log1p(this.train("SalePrice")))
  }

  def modelFit(): Unit = {
    this.gBTRegressor = new GBTRegressor()  // cross validator only features label prediction

    this.paramGrid = new ParamGridBuilder()
      .addGrid(this.gBTRegressor.maxDepth, Array(3, 4, 5))
      .build()

    this.crossValidator = new CrossValidator()
      .setEstimator(this.gBTRegressor)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(this.paramGrid)
      .setNumFolds(5)
      .setParallelism(1)

    val crossValidatorModel: CrossValidatorModel = this.crossValidator.fit(this.train)
    this.test = crossValidatorModel.transform(this.test)
    this.test = this.test.withColumn("SalePrice", expm1(this.test("prediction")))
  }

  def dataWrite(): Unit = {
    this.sampleSubmission = this.test.select(this.test("Id"), this.test("SalePrice") as "SalePrice")
    this.sampleSubmission.repartition(1)
      .write
      .option(key = "header", value = true)
      .csv(this.outputPath + "sampleSubmission")
  }
}

object PipelineTest1 {
  def main(args: Array[String]): Unit = {
    val pt: PipelineTest1 = new PipelineTest1(
      "file:\\C:\\Users\\jiangyilan\\IdeaProjects\\Titanic_And_House_Price\\data\\HousePrice\\",
      "file:\\E:\\Kaggle\\House Price\\"
    )
    pt.dataRead()
    pt.dataPrepare()
    pt.modelFit()
    pt.dataWrite()
  }
}
