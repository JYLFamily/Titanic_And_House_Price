package main.scala.oop

class ExtendsTest extends AbstractTest {
  override val name: String = ""
  override val time: String = "2018"

  override def showName(): String = {
    this.name
  }

  def showTime(): String = {
    this.time
  }
}

object ExtendsTest {
  def main(args: Array[String]): Unit = {
    val et = new ExtendsTest
    println(et.showName)
    println(et.showTime)
  }
}


