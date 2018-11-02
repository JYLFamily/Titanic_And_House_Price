package main.scala.oop

class OopTest2(private var name: String = "") {
  private var birthday: String = ""
  private var idCardNo: String = ""

  def this(name: String, birthday: String, idCardNo: String) {
    this(name)
    this.birthday = birthday
    this.idCardNo = idCardNo
  }

  def gsName: String = this.name
  def gsName_=(name: String): Unit = this.name = name

  def gsBirthday: String = this.birthday
  def gsBirthday_=(birthday: String): Unit = this.birthday = birthday

  def gsIdCardNo: String = this.idCardNo
  def gsIdCardNo_=(idCardNo: String): Unit = this.idCardNo = idCardNo
}
