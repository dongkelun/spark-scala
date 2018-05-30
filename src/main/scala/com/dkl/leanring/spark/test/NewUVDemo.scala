package com.dkl.leanring.spark.test

import org.apache.spark.sql.SparkSession

object NewUVDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("NewUVDemo").master("local").getOrCreate()
    val rdd1 = spark.sparkContext.parallelize(
      Array(
        ("2017-01-01", "a"), ("2017-01-01", "b"), ("2017-01-01", "c"), ("2017-01-01", "c"),
        ("2017-01-02", "a"), ("2017-01-02", "b"), ("2017-01-02", "d"), ("2017-01-02", "d"),
        ("2017-01-03", "b"), ("2017-01-03", "e"), ("2017-01-03", "f")))

    rdd1.distinct().foreach(println)
    return
    val temp = rdd1.groupByKey;
    temp.map(kv => (kv._1, kv._2.min)).foreach(println)
    temp.foreach(println)
    //倒排
    val rdd2 = rdd1.map(kv => (kv._2, kv._1))
    //倒排后的key分组
    val rdd3 = rdd2.groupByKey()
    //取最小时间
    val rdd4 = rdd3.map(kv => (kv._2.min, 1))
    rdd4.countByKey().foreach(println)

  }
}