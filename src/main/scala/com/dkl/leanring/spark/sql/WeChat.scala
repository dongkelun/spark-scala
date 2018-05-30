package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.util.Properties
import java.sql.DriverManager
object WeChat {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDemo").master("local").getOrCreate()

    val url = "jdbc:mysql://192.168.44.128:3306/hive?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC";
    import spark.implicits._
    val df = spark.read.option("header", "true").csv("src/main/resources/scala/wechat.csv")
    df.show()
    val prop = new Properties()

    prop.put("user", "root")
    prop.put("password", "Root-123456")
    df.write.mode(SaveMode.Overwrite).jdbc(url,
      "WECHAT_RESULT", prop)
    val url_ora = "jdbc:oracle:thin:@10.180.24.22:1521:sdhspay";
    prop.put("user", "payadm")
    prop.put("password", "PAYADM")
    df.write.mode(SaveMode.Overwrite).jdbc(url_ora, "WECHAT_RESULT", prop)
  }
}