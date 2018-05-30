package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * spark查询mysql测试
 */
object MysqlQueryDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MysqlQueryDemo").master("local").getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.44.128:3306/hive?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "USER_T")
      .option("user", "root")
      .option("password", "Root-123456")
      .load()
    jdbcDF.show()
  }
}