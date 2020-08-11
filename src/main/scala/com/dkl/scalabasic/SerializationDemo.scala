package com.dkl.scalabasic

import java.io.FileOutputStream
import java.io.ObjectOutputStream

class Hippie(val name: String, @transient val age: Int) extends Serializable

/**
 * 序列化，测试@transient
 */
object SerializationDemo {

  def main(args: Array[String]): Unit = {
    val fos = new FileOutputStream("hippie.txt")
    val oos = new ObjectOutputStream(fos)

    val p1 = new Hippie("zml", 34)
    oos.writeObject(p1)
    oos.close()
  }

}
