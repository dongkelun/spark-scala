package com.dkl.blog.spark.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * 博客：Spark 统计每天新增用户数
 * https://dongkelun.com/2018/04/11/sparkNewUV/
 */
object NewUVDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("NewUVDemo").master("local").getOrCreate()
    val rdd1 = spark.sparkContext.parallelize(
      Array(
        ("2017-01-01", "a"), ("2017-01-01", "b"), ("2017-01-01", "c"),
        ("2017-01-02", "a"), ("2017-01-02", "b"), ("2017-01-02", "d"),
        ("2017-01-03", "b"), ("2017-01-03", "e"), ("2017-01-03", "f")))
    //倒排
    val rdd2 = rdd1.map(kv => (kv._2, kv._1))
    //倒排后的key分组
    val rdd3 = rdd2.groupByKey()
    //取最小时间
    val rdd4 = rdd3.map(kv => (kv._2.min, 1))
    rdd4.countByKey().foreach(println)

  }
}
