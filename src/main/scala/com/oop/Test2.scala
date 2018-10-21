package com.oop

object Test2 {
  def main(args: Array[String]): Unit = {
    val ot = new OopTest2("JYL", "1985910", "110")

    println(ot.gsName)
    println(ot.gsBirthday)
    println(ot.gsIdCardNo)

    println("------------")
    ot.gsName = "JYF"
    ot.gsBirthday = "2000401"
    ot.gsIdCardNo = "105"
    println(ot.gsName)
    println(ot.gsBirthday)
    println(ot.gsIdCardNo)
  }
}
