package com.dkl.learning.spark.rdd

import org.apache.spark.sql.SparkSession

/**
 * Rdd CheckPoint学习
 */
object CheckPointDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setCheckpointDir("hdfs://ambari.master.com:8020/spark/dkl/checkpoint/checkpointdemo")

    val rdd = sc.parallelize(1 to 1000, 10)

    rdd.checkpoint()

    val rdd1 = sc.parallelize(1 to 1000, 2)

    rdd1.checkpoint()
    println(rdd1.max)

    println(rdd.sum)
    println(rdd.count)

    spark.stop()
  }

}
