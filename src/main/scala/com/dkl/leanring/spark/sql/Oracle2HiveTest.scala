package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object Oracle2HiveTest {
  def main(args: Array[String]): Unit = {

    //初始化spark
    val spark = SparkSession
      .builder()
      .appName("Oracle2HiveTest")
      .master("local")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    //表名为我们新建的测试表
    val tableName = "test"

    //spark连接oracle数据库
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@192.168.44.128:1521:orcl")
      .option("dbtable", tableName)
      .option("user", "bigdata")
      .option("password", "bigdata")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
    //导入spark的sql函数，用起来较方便
    import spark.sql
    //切换到test数据库
    sql("use test")
    //将df中的数据保存到hive表中（自动建表）
    df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    //停止spark
    spark.stop
  }
}