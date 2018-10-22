package main.scala.com.oop

object ApplyTest {
  def apply(param1: String, param2: String): String = {
    println("apply method called")
    param1 + " and " + param2
  }

  def main(args: Array[String]): Unit = {
    // print apply method called
    val at = ApplyTest("JYL", "JYF")
    // ApplyTest("JYL", "JYF") 调用了ApplyTest 的 apply 方法，返回 String
    println(at.getClass)
    println(at)
  }
}
