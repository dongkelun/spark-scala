package com.dkl.blog.spark.oracle

import org.apache.spark.sql.SparkSession

/**
 * 博客：spark-submit报错:Exception in thread "main" java.sql.SQLException:No suitable driver
 * https://dongkelun.com/2018/05/06/sparkSubmitException/
 */
object Oracle2HiveDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Oracle2HiveDemo")
      .master("local")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    val allTablesDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@192.168.44.128:1521:orcl")
      .option("dbtable", "(select table_name,owner from all_tables where  owner  in('BIGDATA'))a")
      .option("user", "bigdata")
      .option("password", "bigdata")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
    import spark.implicits._
    import spark.sql
    sql("use oracle_test")
    allTablesDF.rdd.collect().foreach(row => {
      val tableName: String = row(0).toString()
      val dataBase: String = row(1).toString()

      println(dataBase + "." + tableName)
      val df = spark.read
        .format("jdbc")
        .option("url", "jdbc:oracle:thin:@192.168.44.128:1521:orcl")
        .option("dbtable", dataBase + "." + tableName)
        .option("user", "bigdata")
        .option("password", "bigdata")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .load()
      df.write.mode("overwrite").saveAsTable(tableName)

    })

    spark.stop

  }
}
