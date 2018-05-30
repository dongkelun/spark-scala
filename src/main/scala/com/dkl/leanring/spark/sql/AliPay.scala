package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.SaveMode

object AliPay {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JdbcDemo").master("local").getOrCreate()

    import spark.implicits._
    val df = spark.read.option("header", "true").csv("src/main/resources/scala/alipay.csv")
    df.show()
    val prop = new Properties()
    val url = "jdbc:oracle:thin:@10.180.24.22:1521:sdhspay";
    prop.put("user", "payadm")
    prop.put("password", "PAYADM")
    df.write.mode(SaveMode.Overwrite).jdbc(url, "ALIPAY_RESULT", prop)
  }
}