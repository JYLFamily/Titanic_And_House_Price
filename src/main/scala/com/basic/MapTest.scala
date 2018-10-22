package main.scala.com.basic

object MapTest {
  def main(args: Array[String]): Unit = {
    val mutableMap = scala.collection.mutable.Map(
      "XMU" -> "Xiamen University"
    )
    val immutableMap = Map(
      "XMU" -> "Xiamen University"
    )

    mutableMap += ("THU" -> "Tsinghua University")
//    immutableMap += ("THU" -> "Tsinghua University")

    var mutableMapTwo = scala.collection.mutable.Map(
      "XMU" -> "Xiamen University"
    )
    var immutableMapTwo = Map(
      "XMU" -> "Xiamen University"
    )

    mutableMapTwo += ("THU" -> "Tsinghua University")
    immutableMapTwo += ("THU" -> "Tsinghua University")
  }
}
