package com.hs.xlzf.task

import org.apache.spark.sql.SparkSession

object Oracle2HiveDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Oracle2HiveDemo")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .enableHiveSupport()
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@192.168.44.128:1521:orcl")
      .option("dbtable", "(select table_name,owner from all_tables where  owner  in('BIGDATA'))a")
      .option("user", "bigdata")
      .option("password", "bigdata")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
    jdbcDF.show()
    import spark.implicits._
    import spark.sql
    sql("CREATE DATABASE IF NOT EXISTS ORACLE_TEST")
    sql("USE ORACLE_TEST")
    jdbcDF.rdd.collect().foreach(row => {
      val tableName: String = row(0).toString()
      val dataBase: String = row(1).toString()

      println(dataBase + "." + tableName)
      val df = spark.read
        .format("jdbc")
        .option("url", "jdbc:oracle:thin:@192.168.44.128:1521:orcl")
        .option("dbtable", dataBase + "." + tableName)
        .option("user", "bigdata")
        .option("password", "bigdata")
        .load()

      df.write.mode("overwrite").saveAsTable(tableName)

    })

    spark.stop

  }
}