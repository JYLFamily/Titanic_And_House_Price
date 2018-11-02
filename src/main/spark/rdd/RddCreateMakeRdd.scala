package main.spark.rdd

import org.apache.spark.sql.SparkSession

object RddCreateMakeRdd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("RddCreateMakeRdd")
      .getOrCreate()

    val mySeq = List[(String, List[String])](
      ("China Person", List("LiLei", "HanMeiMei")),
      ("American Person", List("Tom", "Jim")),
      ("number Cate", List("one", "two")),
      ("Color Type", List("Red", "Blue"))
    )

    val myRdd = spark.sparkContext
      .makeRDD(mySeq)

    print(myRdd.partitions.size)

    spark.stop()
  }
}
