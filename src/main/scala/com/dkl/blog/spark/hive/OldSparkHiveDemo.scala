package com.dkl.blog.spark.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext

/**
 * 旧版本spark-hive测试
 * 博客：Spark连接Hive（spark-shell和Eclipse两种方式）
 * https://dongkelun.com/2018/03/25/sparkHive/
 */
object OldSparkHiveDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OldSparkHiveDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val hiveCtx = new HiveContext(sc)

    hiveCtx.sql("select * from test").show()
    val data = Array((3, "name3"), (4, "name4"), (5, "name5"))
    val df = sc.parallelize(data).toDF("id", "name")
    df.createOrReplaceTempView("user")
    hiveCtx.sql("insert into test select id,name from user")
    hiveCtx.sql("select * from test").show()
  }

}
