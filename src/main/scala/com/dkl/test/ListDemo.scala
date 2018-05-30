package com.dkl.test

object ListDemo {
  def main(args: Array[String]): Unit = {

    val list = 1 :: Nil

    list.headOption.getOrElse(0) // 1
    val a = Some(list(0)).asInstanceOf[Option[_]].getOrElse(0)
    println(a)
    //获取第1个元素
    list.headOption.getOrElse(0) // 1
    val list1 = List("c", "a", "l", "a")

    //字符串只能在左
    val list2 = "s" :: list1
    println(list2)
    val list3 = List("p", "l", "a", "y")
    val list4 = list3 ::: list2
    println(list4)
    println(list2 ::: list3)
  }
}