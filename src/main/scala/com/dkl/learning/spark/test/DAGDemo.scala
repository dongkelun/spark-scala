package com.dkl.learning.spark.test

import org.apache.spark.sql.SparkSession

object DAGDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("NewUVDemo").master("local").getOrCreate()
    val sc = spark.sparkContext
    val data = Array(
      ("2017-01-01", "a"), ("2017-01-01", "b"), ("2017-01-01", "c"), ("2017-01-01", "c"),
      ("2017-01-02", "a"), ("2017-01-02", "b"), ("2017-01-02", "d"), ("2017-01-02", "d"),
      ("2017-01-03", "b"), ("2017-01-03", "e"), ("2017-01-03", "f"))
    val rdd1 = sc.parallelize(data)
    val rdd2 = rdd1.map((_, 1))
    val rdd3 = rdd2.reduceByKey(_ + _)
    val array = rdd3.collect()
    array.foreach(println)
  }
}
