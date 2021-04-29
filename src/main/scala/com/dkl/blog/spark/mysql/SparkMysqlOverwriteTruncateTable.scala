package com.dkl.blog.spark.mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

/**
 * Created by dongkelun on 2021/4/29 14:06
 *
 * 博客：Spark覆盖写入mysql表但不改变已有的表结构
 * https://dongkelun.com/2021/04/29/SparkMysqlOverwriteTruncateTable/
 *
 */
object SparkMysqlOverwriteTruncateTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkMysqlOverwriteTruncateTable")
      .master("local[*]")
      .getOrCreate()

    //rewriteBatchedStatements参数为批量写入数据，可以增加写入效率
    val url = "jdbc:mysql://192.168.44.128:3306/test?useUnicode=true&characterEncoding=utf-8&rewriteBatchedStatements=true"
    val tableName = "trafficbase_cljbxx"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "Root-123456")

    //读取本地csv
    var df = spark.read.option("header", "true").csv("D:\\文档\\inspur\\csg\\功能测评\\测试数据\\trafficbase_cljbxx.csv")
    //字符串转为日期类型
    df = df.withColumn("czsj", to_date(col("czsj"), "dd/mm/yyyy"))
    df.show()

    df.write.mode("overwrite")
      .option("truncate", true) //覆盖写入数据前先truncate table而不是drop table
      .jdbc(url = url, table = tableName, prop)
    spark.stop
  }
}
