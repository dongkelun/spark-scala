package com.dkl.blog.spark.hive

import org.apache.spark.sql.SparkSession

/**
  * Created by dongkelun on 2020/1/16 15:25
  *
  * 博客：Spark 覆盖写Hive分区表,只覆盖部分对应分区
  *
  * 要求Spark版本2.3以上
  */
object SparkHivePartitionOverwrite {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkHivePartitionOverwrite")
      .master("local")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .config("spark.sql.sources.partitionOverwriteMode","dynamic")
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    val data = Array(("001", "张三", 21, "2018"), ("002", "李四", 18, "2017"))

    val df = spark.createDataFrame(data).toDF("id", "name", "age", "year")
    //创建临时表
    df.createOrReplaceTempView("temp_table")

    val tableName="test_partition"
    //切换hive的数据库
    sql("use dkl")
    //    1、创建分区表，可以将append改为overwrite，这样如果表已存在会删掉之前的表，新建表
    df.write.mode("overwrite").partitionBy("year").saveAsTable(tableName)

    spark.table(tableName).show()

    val data1 = Array(("011", "Sam", 21, "2018"))

    val df1 = spark.createDataFrame(data1).toDF("id", "name", "age", "year")
//    df1.write.mode("overwrite").partitionBy("year").saveAsTable(tableName)    //不成功，全表覆盖
//    df1.write.mode("overwrite").format("Hive").partitionBy("year").saveAsTable(tableName) //不成功，全表覆盖
    df1.write.mode("overwrite").insertInto(tableName)

    spark.table(tableName).show()

    spark.stop
  }

}
