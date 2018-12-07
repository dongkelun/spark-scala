package com.dkl.learning.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.util.Properties

object JdbcDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDemo").master("local").getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.44.128:3306/hive?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC")
      .option("dbtable", "USER_T")
      .option("fetchSize", "1")
      .option("user", "root")
      .option("password", "Root-123456")
      .load()

    jdbcDF.show()
    val jdbcDF1 = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.44.128:3306/hive?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC")
      .option("dbtable", "(select count(*) from USER_T)a")
      .option("fetchSize", "1")
      .option("user", "root")
      .option("password", "Root-123456")
      .load()
    jdbcDF1.show()

    import spark.implicits._
    val prop = new Properties()
    val data = Array((15, "test", "test", 24), (16, "test", "test", 24), (17, "test", "test", 24))
    var df = spark.createDataFrame(data).toDF("ID", "USER_NAME", "PASSWORD", "AGE")

    prop.put("user", "root")
    prop.put("password", "Root-123456")
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.44.128:3306/hive?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC",
      "USER_T", prop)
  }

}
