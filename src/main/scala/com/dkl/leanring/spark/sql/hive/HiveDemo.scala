package com.dkl.leanring.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object HiveDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Oracle2HiveDemo")
      .master("local")
      //.config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val s1 = """{"name":21}"""
    val s2 = """{"name":"32"}"""
    val ss = Seq(s1, s2)
    val ds = spark.createDataset(ss)

    val a = Array("1", "2", "3")
    val sc = spark.sparkContext
    val rdd = sc.parallelize(a, 1)

    val res = rdd.reduce(_ + _)
    println(res)

    ds.show()
    ds.printSchema()
    val df1 = spark.read.json(ds)
    df1.show()
    df1.printSchema()
    val data = Array(("1", "张三"), ("2", "李四"))
    val df = spark.createDataFrame(data).toDF("id", "name")
    df.printSchema()
    df.show()

    //    import spark.sql
    //    df.createTempView("test_df")
    //    sql("use dkl")
    //    sql("insert into test select * from test_df")
    //    df.write.mode(SaveMode.Append).saveAsTable("test")

    spark.stop()
  }

}