package com.dkl.scalabasic

/**
 * Some类使用示例
 */
object SomeDemo {

  def main(args: Array[String]): Unit = {
    val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo", "China" -> "Beijing")

    println(capitals get "France")
    println(capitals.get("France"))
    println(capitals get "North Pole")
    println(showCapital(capitals get "France"))

  }

  def showCapital(x: Option[String]) = x match {
    case Some(s) => s
    case None => "?"
  }
}
