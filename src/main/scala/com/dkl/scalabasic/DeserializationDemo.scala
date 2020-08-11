package com.dkl.scalabasic

import java.io.FileInputStream
import java.io.ObjectInputStream

/**
 * 反序列化，测试@transient
 */
object DeserializationDemo {
  def main(args: Array[String]): Unit = {
    val fis = new FileInputStream("hippie.txt")
    val ois = new ObjectInputStream(fis)

    val hippy = ois.readObject.asInstanceOf[Hippie]
    println(hippy.name)
    println(hippy.age)
    ois.close()

  }

}
