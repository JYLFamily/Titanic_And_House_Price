package com.mat

object MatchTest {
  def main(args: Array[String]): Unit = {
    for (elem <- List(9, 12.3, "spark", "hadoop", "hello")) {
      val str = elem match {
        case Int => " is an int value."
        case Double => " is a double value."
          // 报错
//        case String => " is a string value."
      }
      println(str)
    }
  }
}
