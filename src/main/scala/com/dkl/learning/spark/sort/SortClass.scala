package com.dkl.learning.spark.sort

import org.apache.spark.sql.SparkSession

object SortClass {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val sc = spark.sparkContext
    val classSore = sc.textFile("files/learning/class.txt", 1)
    classSore.saveAsTextFile("files/learning/test")

    val rdd1 = classSore.map(_.split(" ")).map(kv => (kv(0), kv(1).toInt)).groupByKey()
    //    val data = sc.textFile("files/learning/data.txt", 1)
    //    val res = data.filter(_.toCharArray().filter(_ == '8').length >= 3)
    //    res.foreach(println)
    val res = rdd1.map(kv => {
      val score = kv._2.toArray
      score.sortWith(_ > _).take(3)
      (kv._1, score.sortWith(_ > _).take(3))

    })
    res.foreach(kv => {
      println(kv._1)
      kv._2.foreach(println)
    })
    spark.stop()
    spark.close()
  }

}
