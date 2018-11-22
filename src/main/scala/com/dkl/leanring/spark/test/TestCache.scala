package com.dkl.leanring.spark.test

import org.apache.spark.sql.SparkSession

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
