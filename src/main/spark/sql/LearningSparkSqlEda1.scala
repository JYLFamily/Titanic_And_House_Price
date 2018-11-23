package main.spark.sql

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.SparkSession

object LearningSparkSqlEda1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("CreateDataFrame4")
      .master("local")
      .config("spark.sql.warehouse.dir", "E:\\Spark\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._

    val dfMissing  = spark.read
      .option("header", true)
      .option("sep", ";")
      .option("nullValue", "unknown")  // 只能有一组 nullValue
      .option("inferSchema", true)
      .csv("file:\\C:\\Users\\jiangyilan\\IdeaProjects\\Titanic_And_House_Price\\data\\LearningSparkSql\\bank-additional-full.csv")

    val dsMissing = dfMissing.as[Call]

    dfMissing.show

    dsMissing.stat
      .crosstab("age", "marital")
      .orderBy("age_marital")
      .show

    spark.stop()
  }
}
