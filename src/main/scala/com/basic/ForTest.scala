package com.basic

object ForTest {
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 5; j <- i to 5) {
      println(i * j)
    }

    println("------------")
    val list = for (i <- 1 to 10 if i % 2 == 0) yield i
    println(list)
  }
}
