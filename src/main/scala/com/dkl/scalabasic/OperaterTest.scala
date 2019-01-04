package com.dkl.scalabasic

/**
 * 自定义操作符
 * 操作符默认左结合,除了以:结尾的操作符是右结合调用,比如::
 */

object OperaterTest extends App {

  override def main(args: Array[String]): Unit = {
    val a: myInt = new myInt(1)
    val b: myInt = new myInt(2)
    val c: myInt = new myInt(3)

    println(a +++ b)
    println((c ---: b ---: a).value) //：结尾的操作符右结合,相当于(a.---:(b)).---:(c) = 1-2-3
    println(((a.---:(b)).---:(c)).value)
    println(c --: b)

  }

}
class myInt(val value: Int) {
  def +++(a: myInt): Int = { // 定义操作符 +++
    a.value + value // 要使a.value能够访问，主构造器的字段要命名成val，使getter,setter方法为public
  }

  // 操作符默认左结合,除了以:结尾的操作符是右结合调用,比如::
  def ---:(a: myInt): myInt = { // 定义操作符 ---:
    new myInt(value - a.value)
  }
  def --:(a: myInt): Int = { // 定义操作符 --:
    value - a.value
  }

}
