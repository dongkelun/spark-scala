package com.dkl.blog.spark.exception

import org.apache.spark.sql.SparkSession

/**
 * 博客： Spark 持久化（cache和persist的区别）
 * https://dongkelun.com/2018/06/03/sparkCacheAndPersist/
 */
object TestCache {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile("files/test.txt")
    var beiginTime = System.currentTimeMillis
    println(rdd.count)
    var endTime = System.currentTimeMillis
    println("cost " + (endTime - beiginTime) + " milliseconds.")

    beiginTime = System.currentTimeMillis
    println(rdd.count)
    endTime = System.currentTimeMillis
    println("cost " + (endTime - beiginTime) + " milliseconds.")
    spark.stop
  }
}
