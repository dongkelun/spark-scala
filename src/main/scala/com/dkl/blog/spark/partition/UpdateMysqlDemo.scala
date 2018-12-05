package com.dkl.blog.spark.partition

import org.apache.spark.sql.SparkSession

/**
 * Spark更新mysql的几种方法示例（博客用例）
 *
 * 博客：Spark性能优化：基于分区进行操作
 * 博客地址：https://dongkelun.com/2018/09/02/sparkMapPartitions/
 */
object UpdateMysqlDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UpdateMysqlDemo").master("local").getOrCreate()

    val database_url = "jdbc:mysql://192.168.44.128:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val user = "root"
    val password = "Root-123456"
    val df = spark.read
      .format("jdbc")
      .option("url", database_url)
      .option("dbtable", "(select * from test where isDeal=0 limit 30)a")
      .option("user", user)
      .option("password", password)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("numPartitions", "5")
      .option("partitionColumn", "ID")
      .option("lowerBound", "6")
      .option("upperBound", "10")
      .load()

    import java.sql.{ Connection, DriverManager, ResultSet };
    //    df.rdd.foreach(row => {
    //      val conn = DriverManager.getConnection(database_url, user, password)
    //      try {
    //        // Configure to be Read Only
    //        val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    //        val prep = conn.prepareStatement(s"update test set isDeal=1 where id=?")
    //
    //        val id = row.getAs[Int]("id")
    //        prep.setInt(1, id)
    //        prep.executeUpdate
    //
    //      } catch {
    //        case e: Exception => e.printStackTrace
    //      } finally {
    //        conn.close()
    //      }
    //
    //    })

    val conn = DriverManager.getConnection(database_url, user, password)
    try {
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val prep = conn.prepareStatement(s"update test set isDeal=1 where id=?")

      df.select("id").collect().foreach(row => {
        val id = row.getAs[Int]("id")
        prep.setInt(1, id)
        prep.executeUpdate

      })

    } catch {
      case e: Exception => e.printStackTrace
    }

    df.rdd.foreachPartition(it => {
      val conn = DriverManager.getConnection(database_url, user, password)
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        val prep = conn.prepareStatement(s"update test set isDeal=1 where id=?")
        it.foreach(row => {
          val id = row.getAs[Int]("id")
          prep.setInt(1, id)
          prep.executeUpdate
        })

      } catch {
        case e: Exception => e.printStackTrace
      } finally {
        conn.close()
      }

    })

    spark.stop()
  }
}
