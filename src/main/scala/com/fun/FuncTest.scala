package main.scala.com.fun

object FuncTest {
  def main(args: Array[String]): Unit = {
    val numList = List(-5, -3, -1, 3, 5, 7, 9)
    numList.filter(x => x > 0).foreach(print)
    println()
    println("------------------------")
  }
}
