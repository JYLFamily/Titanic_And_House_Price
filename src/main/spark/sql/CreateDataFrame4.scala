package main.spark.sql

import org.apache.spark.sql.SparkSession

object CreateDataFrame4 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("CreateDataFrame4")
      .master("local")
      .config("spark.sql.warehouse.dir", "E:\\Spark\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    // DataFrame DataSet[Row] untyped
    val dataFrame = spark.read
      .option("header", true)
      .option("nullValue", "NA")  // 只能有一组 nullValue
      .option("inferSchema", true)
      .csv("file:\\C:\\Users\\jiangyilan\\IdeaProjects\\Titanic_And_House_Price\\data\\HousePrice\\train.csv")

    // dataPrepare DataFrame API
    dataFrame.select($"Id").show

    // dataPrepare DataFrame sql API return DataFrame
    dataFrame.createOrReplaceTempView("train")
    spark.sql("SELECT Id FROM train").show

    // DataSet typed
    val dataSet1 = spark.sparkContext.parallelize(List[(Int, Long, String)](
      (26, 1, "JYL"),
      (18, 2, "JYF"),
      (30, 3, "JHJ")
    ))
      .map(tuple => Sample(tuple._1, tuple._2, tuple._3))
      .toDS()
    dataSet1.show()

//    val typedTransformation = dataSet1.filter(sample => sample.Id == "26")
//    val typedTransformation = dataSet1.toDF().filter(row => row.getString(0) == "26")

    val dataSet2 = dataFrame.select($"Id", $"MSSubClass", $"MSZoning").as[Sample]
    dataSet2.show

    spark.stop()
  }
}