package main.scala.com.oop

object Test {
  def main(args: Array[String]): Unit = {
    val ot = new OopTest
    println(ot.current())

    ot.value = 5
    println(ot.current())

    ot.increment()
    ot.increment()
    println(ot.current())
  }
}
