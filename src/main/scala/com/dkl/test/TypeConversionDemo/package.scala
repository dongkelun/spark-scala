package com.dkl.test

package object TypeConversionDemo {
  def main(args: Array[String]): Unit = {
    val strList: List[String] = List("a", "b", "c")
    val strToIntList: List[Int] = strList.asInstanceOf[List[Int]]
    val head = strToIntList(0)
    println(head)

  }
}

object Test extends App {
  val strList: List[String] = List("a", "b", "c")
  val strToIntList: List[Int] = strList.asInstanceOf[List[Int]]

  def pr() {
    println(strToIntList)
  }
}