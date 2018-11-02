package main.spark.rdd

import org.apache.spark.Partitioner

class Partition(numParts: Int) extends Partitioner {

  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    key.toString.toInt % 10
  }
}
