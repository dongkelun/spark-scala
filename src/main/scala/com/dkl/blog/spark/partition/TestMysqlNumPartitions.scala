package com.dkl.blog.spark.partition

import org.apache.spark.sql.SparkSession
import org.apache.spark.annotation.InterfaceStability

/**
 * 测试读取mysql数据库得到的DataFrame的默认分区数（默认1）（博客用例）
 * 博客地址：https://dongkelun.com/2018/08/13/sparkDefaultPartitionNums/
 */
object TestMysqlNumPartitions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TestMysqlNumPartitions").master("local[80]").getOrCreate()

    val database_url = "jdbc:mysql://10.180.29.181:3306/route_analysis?useUnicode=true&characterEncoding=utf-8"
    val user = "route"
    val password = "Route-123"
    val df = spark.read
      .format("jdbc")
      .option("url", database_url)
      .option("dbtable", "service_freq_step_user")
      .option("user", user)
      .option("password", password)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    println(spark.sparkContext.defaultParallelism)

    println(df.rdd.getNumPartitions)

    spark.stop()

  }
}
