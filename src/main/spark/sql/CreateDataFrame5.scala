package main.spark.sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object CreateDataFrame5 {
  def main(args: Array[String]): Unit = {
    // read DataFrame 1 fast
    val spark = SparkSession.builder()
      .appName("CreateDataFrame5")
      .master("local")
      .config("spark.sql.warehouse.dir", "E:\\Spark\\spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val trainSchema = StructType(Array(
      StructField("PassengerId", IntegerType, nullable = false),
      StructField("Survived", IntegerType, nullable = false),
      StructField("Pclass", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Sex", StringType, nullable = true),
      StructField("Age", DoubleType, nullable = true),
      StructField("SibSp", DoubleType, nullable = true),
      StructField("Parch", DoubleType, nullable = true),
      StructField("Ticket", StringType, nullable = true),
      StructField("Fare", DoubleType, nullable = true),
      StructField("Cabin", StringType, nullable = true),
      StructField("Embarked", StringType, nullable = true)
    ))

    val testSchema = StructType(Array(
      StructField("PassengerId", IntegerType, nullable = false),
      StructField("Pclass", StringType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Sex", StringType, nullable = true),
      StructField("Age", DoubleType, nullable = true),
      StructField("SibSp", DoubleType, nullable = true),
      StructField("Parch", DoubleType, nullable = true),
      StructField("Ticket", StringType, nullable = true),
      StructField("Fare", DoubleType, nullable = true),
      StructField("Cabin", StringType, nullable = true),
      StructField("Embarked", StringType, nullable = true)
    ))

    val trainOne = spark.read
      .schema(trainSchema)
      .option("header", true)
      .option("nanValue", "")
      .csv("file:\\C:\\Users\\jiangyilan\\IdeaProjects\\Titanic_And_House_Price\\data\\Titanic\\train.csv")

    val testOne = spark.read
      .schema(testSchema)
      .option("header", true)
      .option("nanValue", "")
      .csv("file:\\C:\\Users\\jiangyilan\\IdeaProjects\\Titanic_And_House_Price\\data\\Titanic\\test.csv")

    trainOne.show
    testOne.show

    // read DataFrame 2 slow
    val trainTwo = spark.read
        .option("header", true)
        .csv("file:\\C:\\Users\\jiangyilan\\IdeaProjects\\Titanic_And_House_Price\\data\\Titanic\\train.csv")

    val testTwo = spark.read
        .option("header", true)
        .csv("file:\\C:\\Users\\jiangyilan\\IdeaProjects\\Titanic_And_House_Price\\data\\Titanic\\test.csv")

    trainTwo.show
    testTwo.show


    spark.stop()
  }
}
