package com.dkl.blog.spark.hive

import org.apache.spark.sql.SparkSession

/**
 * Spark连接Hive的一些测试
 */
object SparkHive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkHive")
      .master("local")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.table("dkl.test_partition")

    df.show

    spark.stop
  }
}
