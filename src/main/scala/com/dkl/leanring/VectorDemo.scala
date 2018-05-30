package com.dkl.leanring
import com.atomicscala.AtomicTest._

object VectorDemo {
  def main(args: Array[String]): Unit = {
    val v1 = Vector(1, 3, 5, 7, 11);
    v1 is Vector(1, 3, 5, 7, 11)

    v1(4) is 11
  }
}