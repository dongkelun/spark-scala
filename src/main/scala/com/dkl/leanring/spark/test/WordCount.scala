package com.dkl.leanring.spark.test

import org.apache.spark.sql.SparkSession

/**
 * 同时用map和mapPartitions实现WordCount，看一下mapPartitions的用法以及与map的区别
 * 博客：https://dongkelun.com/2018/09/02/sparkMapPartitions/
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("WordCount").getOrCreate()
    val sc = spark.sparkContext

    val input = sc.parallelize(Seq("Spark Hive Kafka", "Hadoop Kafka Hive Hbase", "Java Scala Spark"), 10)
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey { (x, y) => x + y }
    println(counts.collect().mkString(","))
    val counts1 = words.mapPartitions(it => it.map(word => (word, 1))).reduceByKey { (x, y) => x + y }
    println(counts1.collect().mkString(","))

    val rdd = sc.parallelize(1 to 10, 5)
    val res = rdd.mapPartitionsWithIndex((index, it) => {
      it.map(n => (index, n * n))
    })
    println(res.collect().mkString(" "))
    rdd.foreachPartition(it => it.foreach(println))

    spark.stop()

  }
}
