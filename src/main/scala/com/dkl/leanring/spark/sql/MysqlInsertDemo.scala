package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.util.Properties

/**
 * 从USER_T.csv读取数据并插入的mysql表中
 */
object MysqlInsertDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MysqlInsertDemo").master("local").getOrCreate()
    val df = spark.read.option("header", "true").csv("src/main/resources/scala/USER_T.csv")
    df.show()
    val url = "jdbc:mysql://192.168.44.128:3306/hive?useUnicode=true&characterEncoding=utf-8"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "Root-123456")
    df.write.mode(SaveMode.Append).jdbc(url, "USER_T", prop)
  }
}