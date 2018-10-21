package com.oop

class OopTest3(private var name: String, private var id: Int = OopTest3.newPersonId()) {

  def gsId(): Int = {
    this.id
  }

  def gsId_=(id: Int): Unit = {
    this.id = id
  }

  def gsName(): String = {
    this.name
  }

  def gsName_=(name: String): Unit = {
    this.name = name
  }

}

object OopTest3 {
  private var lastId = 0

  private def newPersonId(): Int = {
    lastId += 1
    lastId
  }

  def main(args: Array[String]): Unit = {
    val ot1 = new OopTest3("JYL")
    val ot2 = new OopTest3("JYF")

    // object 与 class 能够互相访问私有成员变量、方法
    println(ot1.name + ", " + ot1.id)
    println(ot2.name + ", " + ot2.id)

    // class 的 getter 方法
    println(ot1.gsName + ", " + ot1.gsId)
    println(ot2.gsName + ", " + ot2.gsId)
  }
}
