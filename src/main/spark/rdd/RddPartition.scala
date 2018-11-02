package main.spark.rdd

import org.apache.spark.sql.SparkSession

object RddPartition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("RddPartition")
      .getOrCreate()

    val rddData = spark
      .sparkContext
      .parallelize(1 to 5, 5)

//  FIX partitionBy 是怎么实现分区的? 根据 Pari Rdd 的 key
    rddData
      .map(element => (element, 1))
      .partitionBy(new Partition(10))
      .map(tuple => tuple._1)
      .saveAsTextFile("file:\\C:\\Users\\jiangyilan\\Desktop\\output")

    spark.stop()
  }
}
