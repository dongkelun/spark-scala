package com.dkl.scalabasic

abstract class Monoid[A] {
  def add(x: A, y: A): A
  def unit: A
}

/**
 * 隐式参数测试
 */
object ImplicitTest {
  implicit val stringMonoid: Monoid[String] = new Monoid[String] {
    override def add(x: String, y: String): String = x concat y
    override def unit: String = ""
  }

  implicit val intMonoid: Monoid[Int] = new Monoid[Int] {
    override def add(x: Int, y: Int): Int = x + y
    override def unit: Int = 0
  }

  def sum[A](xs: List[A])(implicit m: Monoid[A]): A =
    if (xs.isEmpty) m.unit
    else m.add(xs.head, sum(xs.tail))

  def fun(a: Int)(implicit b: Int = 0) = {
    println(a, b)
  }
  implicit val implicitInt: Int = 10
  //  implicit val implicitInt2: Int = 10
  def main(args: Array[String]): Unit = {
    println(sum(List(1, 2, 3))) // uses IntMonoid implicitly
    println(sum(List("a", "b", "c"))) // uses StringMonoid implicitly

    //    fun(1)
  }
}
