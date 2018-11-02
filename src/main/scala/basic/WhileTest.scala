package main.scala.basic

object WhileTest {
  def main(args: Array[String]): Unit = {
    var element = 10

    do {
      element -= 1
      println(element)
    } while(element > 0)

    println("------------")
    println(element)
  }
}
