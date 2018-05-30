package com.dkl.leanring.spark.map

import org.apache.spark.sql.SparkSession

/**
 * 两个map合并，key一样时值相加
 */
object MapAddDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MapAddDemo").master("local").getOrCreate()
    (List(1, 2, 3, 4, 5) :\ 0)((sum, i) => {
      println(s"sum=${sum} i=${i}")
      sum
    })
    (0 /: List(1, 2, 3, 4))(_ + _)
    val map1 = Map("key1" -> 1, "key2" -> 3, "key3" -> 5)
    map1.map { case (key, count) => println(key, count); key -> (count + map1.getOrElse(key, 0)) }
    map1.map { t => println(t._1, t._2) }
    println(map1.map { t => 2 })
    println(map1.map { t => t._1 -> t._2 })
    val map2 = Map("key2" -> 4, "key3" -> 6, "key5" -> 10)
    val mapAdd = map1 ++ map2.map(t => t._1 -> (t._2 + map1.getOrElse(t._1, 0)))
    println(mapAdd)

    println(map1 + ("key1" -> 3))
    println(map1 ++ map2)
    (map1 /: map2)((map, kv) => {
      println(s"map=${map} kv=${kv}")
      map
    })

    (map1 /: map2) {
      case (map, k) => {
        println(s"map=${map} k=${k}")
        map
      }
    }
    val mapAdd1 = (map1 /: map2)((map, kv) => {
      map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0)))
    })
    println(mapAdd1)

    val m = Map(1 -> (1, 2), 2 -> (2, 3))

    println(m)

    val a = m(1)
    a.productIterator.foreach(println)
    println(a)
    println(m(1))
    import org.apache.spark.sql.functions._
    val a1: Long = 3
    val a2: Long = 4
    println(a1 + a2)
    val map3: Map[String, Int] = Map("key1" -> 1, "key2" -> 3, "key3" -> 5)
    val map4: Map[String, Int] = Map("key2" -> 4, "key3" -> 6, "key5" -> 10)
    val addMap = udf(
      (a: Map[String, Int], b: Map[String, Int]) =>
        a ++ b.map { case (key, count) => key -> (count + a.getOrElse(key, 0)) })
    import spark.implicits._
    val data = Array((map3, map4))
    var df = spark.createDataFrame(data).toDF("col1", "col2")
    df.show()
    val df1 = df.withColumn("col3", addMap(df("col1"), df("col2")))
    df1.select("col3").foreach(row => println(row(0)))
    def addMap1(mapleft: Map[String, Int], mapright: Map[String, Int]): Map[String, Int] = {
      (mapleft /: mapright)((map, kv) => {
        map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0)))
      })
    }

    println(addMap1(map3, map4))
    val mapAdd2 = map1 ++ map2.map { case (key, value) => key -> (value + map1.getOrElse(key, 0)) }
    println(mapAdd2)
    val mapAdd3 = (map1 /: map2) {
      case (map, kv) => {
        map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0)))
      }
    }
    println(mapAdd3)
    val mapAdd4 = (map1 /: map2) {
      case (map, (k, v)) => {
        map + (k -> (v + map.getOrElse(k, 0)))
      }
    }
    println(mapAdd4)
    println(map1 ++ map2.map { case (key, value) => key -> (value + map1.getOrElse(key, 0)) })
  }

}