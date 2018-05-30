package com.dkl.test

object OptionDemo {
  def main(args: Array[String]): Unit = {
    val opt: Option[String] = Some("hello")
    //判断是否为None
    println(opt.isEmpty) // false
    //如果为None，则返回默认值"default"，否则返回opt持有的值
    println(opt.getOrElse("default"))
    //如果为None则返回"DEFAULT"，否则将字符转为大写
    println(opt.fold("DEFAULT") { value => value.toUpperCase }) // "HELLO"
    //功能同上
    val res = opt match {
      case Some(v) => v.toUpperCase
      case None => "DEFAULT"
    }
    println(res)
  }
}