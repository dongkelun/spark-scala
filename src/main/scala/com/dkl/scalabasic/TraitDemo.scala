package com.dkl.scalabasic

/**
 * trait 练习
 *
 * 特质 (Traits) 用于在类 (Class)之间共享程序接口 (Interface)和字段 (Fields)。
 * 它们类似于Java 8的接口。
 * 类和对象 (Objects)可以扩展特质，但是特质不能被实例化，因此特质没有参数。
 */

trait HairColor
trait Iterator[A] {
  def hasNext: Boolean
  def next(): A
}

class IntIterator(to: Int) extends Iterator[Int] with HairColor {
  private var current = 0
  override def hasNext: Boolean = current < to
  override def next(): Int = {
    if (hasNext) {
      val t = current
      current += 1
      t
    } else 0
  }
}

object TraitDemo {
  def main(args: Array[String]): Unit = {
    val iterator = new IntIterator(10)

    println(iterator.next())

    for (i <- 0 until 20) {
      println(i, iterator.next())
    }

  }

}
