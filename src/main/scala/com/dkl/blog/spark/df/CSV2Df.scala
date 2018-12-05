package com.dkl.blog.spark.df

import org.apache.spark.sql.SparkSession

/**
 * 博客：旧版Spark（1.6版本） 将RDD动态转为DataFrame
 * https://dongkelun.com/2018/05/11/rdd2df/
 */
object CSV2Df {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CSV2Df").master("local").getOrCreate()

    val data_path = "files/data.csv"
    val df = spark.read.option("header", "true").csv(data_path)
    df.show()

    spark.stop
  }
}
