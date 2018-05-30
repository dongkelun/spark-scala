package com.dkl.leanring.spark.map

object MapDemo {
  (0 /: List(1, 2, 3, 4))((sum, i) => {
    println(s"sum=${sum} i=${i}")
    sum + i
  })
  def main(args: Array[String]): Unit = {
    // 空哈希表，键为字符串，值为整型
    var A: Map[Char, Int] = Map()
    println(A.isEmpty)
    // Map 键值对演示
    val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF")
    println(colors)
    A += ('I' -> 1)
    A += ('J' -> 5)
    A += ('K' -> 10)
    A += ('L' -> 100)
    println(A.keys)
    println(A.values)
    val colors1 = Map("red" -> "#FF0000",
      "azure" -> "#F0FFFF",
      "peru" -> "#CD853F")
    val colors2 = Map("blue" -> "#0033FF",
      "yellow" -> "#FFFF00",
      "red" -> "#FF0002")
    //  ++ 作为运算符
    var colors3 = colors2 ++ colors1
    println("colors2 ++ colors1 : " + colors3)
    colors3.foreach(i => println(i))
    colors3.keys.foreach {
      i =>
        print(i + " ")
        println(colors3(i))
    }

    val map1 = Map("key1" -> 1, "key2" -> 3)
    val map2 = Map("key2" -> 4, "key3" -> 6)
    val map3 = (map1 /: map2)((map, kv) => {
      map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0)))
    })
    println(map3)
    List(1, 2, 3, 4).foldLeft(0)((sum, i) => sum + i)
    (List(1, 2, 3, 4) foldLeft 0)((sum, i) => sum + i)
    println((0 /: List(1, 2, 3, 4))(_ + _))
    (0 /: List(1, 2, 3, 4))((sum, i) => {
      println(s"sum=${sum} i=${i}")
      sum
    })

    println(map3 + ("key1" -> 2))

  }
}