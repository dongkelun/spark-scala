package com.dkl.learning.spark.test

import org.apache.spark.sql.SparkSession

object TestPartitions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DelFirstNLines").master("local").getOrCreate()

    val sc = spark.sparkContext

    //val path = "files/data.txt"
    val path = "hdfs://ambari.master.com:8020/tmp/dkl/data.txt"
    //    val path = "hdfs://ambari.master.com:8020/tmp/dkl/t1.txt"
    val rdd = sc.textFile(path, 8)
    println("***********")
    rdd.repartition(3).take(3).foreach(println)
    println("***********")
    println("分区数：" + rdd.getNumPartitions)
    val arr = rdd.take(3)
    arr.foreach(println)
    val first = rdd.first()
    val rdd3 = rdd.filter(!arr.contains(_))
    rdd3.foreach(println)
    spark.stop()
  }
}
