package com.dkl.scalabasic

/**
 * 私有成员和Getter/Setter语法
 *
 * 成员默认是公有（public）的。使用private访问修饰符可以在函数外部隐藏它们。
 */
class Point {
  private var _x = 0;
  private var _y = 0;
  private val bound = 100;

  def x = _x;
  def y = _y;

  def x_=(newValue: Int) {
    if (newValue < bound) _x = newValue else printWarning
  }
  def y_=(newValue: Int) {
    if (newValue < bound) _y = newValue else printWarning
  }

  def printWarning = println("WARNING: Out of bounds")

  override def toString(): String = {

    "x: " + x + " y: " + y
  }

}

object GetSetDemo {
  def main(args: Array[String]): Unit = {
    val point1 = new Point
    point1.x = 99
    point1.y = 101 // prints the warning
    println(point1.toString())
  }
}
