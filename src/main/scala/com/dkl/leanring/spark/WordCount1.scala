package com.dkl.leanring.spark

import org.apache.spark.SparkContext

object WordCount1 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "WordCount")
    val rdd1 = sc.textFile("files/abc.txt")
    val words = rdd1.flatMap(x => x.split(" "))
    val result = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    result.sortByKey().foreach(f => println(f._1, f._2))
    println(result.count())
    println(result.sortByKey().collect().mkString(","))
  }
}