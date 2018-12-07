package com.dkl.learning.spark.test

object ScalaFunDemo {
  def main(args: Array[String]): Unit = {

    println(fun1(10))
    println(fun3(10))
    println(fun4(10))
    println(fun4())
    println(sum(1 to 5: _*))
  }

  def fun1(age: Int) {
  }
  def fun3(age: Int) = {
    age
  }
  def fun4(age: Int = 30): Int = {
    age
  }
  def sum(nums: Int*): Int = {
    if (nums.length == 1)
      nums.head
    else
      sum(nums.head) + sum(nums.tail: _*)

  }
}
