package com.dkl.blog.spark.hive

import org.apache.spark.sql.SparkSession

/**
 * 博客：Spark操作Hive分区表
 * https://dongkelun.com/2018/12/04/sparkHivePatition/
 *
 */
object SparkHivePatition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkHive")
      .master("local")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql

    val data = Array(("001", "张三", 21, "2018"), ("002", "李四", 18, "2017"))

    val df = spark.createDataFrame(data).toDF("id", "name", "age", "year")
    //创建临时表
    df.createOrReplaceTempView("temp_table")

    //切换hive的数据库
    sql("use dkl")
    //    1、创建分区表，可以将append改为overwrite，这样如果表已存在会删掉之前的表，新建表
    df.write.mode("append").partitionBy("year").saveAsTable("new_test_partition")
    //2、向Spark创建的分区表写入数据
    df.write.mode("append").partitionBy("year").saveAsTable("new_test_partition")
    sql("insert into new_test_partition select * from temp_table")
    df.write.insertInto("new_test_partition")

    //开启动态分区
    sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //3、向在Hive里用Sql创建的分区表写入数据，抛出异常
    //    df.write.mode("append").partitionBy("year").saveAsTable("test_partition")

    // 4、解决方法
    df.write.mode("append").format("Hive").partitionBy("year").saveAsTable("test_partition")

    sql("insert into test_partition select * from temp_table")
    df.write.insertInto("test_partition")
    //这样会抛出异常
    //    df.write.partitionBy("year").insertInto("test_partition")

    spark.stop
  }
}
