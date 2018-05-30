package com.dkl.test

object StringDemo {
  def main(args: Array[String]): Unit = {
    val s1 = "hello world"
    println(s1)

    //三个引号里面的可以换行，可以包含任意个引号",不需要转移
    val s2 = """Welcome here.
   Type "HELP" for help!"""
    println(s2)

    //判断字符串相等直接用"=="，而不需要使用equals方法
    val s3 = new String("a")
    println(s3 == "a") // true
    //字符串去重
    println("aabbcc".distinct)
    //取前n个字符，如果n大于字符串长度返回原字符串
    println(s2.take(5))
    //过滤特定字符
    println("bcad".filter(_ != 'a')) // "bcd"
    //字符串插值, 以s开头的字符串内部可以直接插入变量，方便字符串构造
    val i = 100
    println(s"i=${i}") // "i=100"
  }
}