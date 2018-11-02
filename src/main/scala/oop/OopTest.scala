package main.scala.oop

class OopTest {
  private var privateValue = 0

  def value: Int = privateValue

  def value_=(value: Int): Unit = {
    this.privateValue = value
  }

  def increment(): Unit = {
    this.privateValue = this.privateValue + 1
  }

  def current(): Int = {
    privateValue
  }
}
