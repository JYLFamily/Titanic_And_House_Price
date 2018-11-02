package main.scala.oop

object UpdateTest {
  def main(args: Array[String]): Unit = {
//    val array = Array[String](3)
//    array(0) = "Hello"

    // update 方法针对伴生类
    val array = new Array[String](3)
    array(0) = "Hadoop"
    array(1) = "Spark"
    array(2) = "Flink"
  }
}
