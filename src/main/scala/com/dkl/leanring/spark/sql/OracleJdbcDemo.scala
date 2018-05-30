package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession

object OracleJdbcDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OracleJdbcDemo").master("local").getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@10.180.24.22:1521:sdhspay")
      .option("dbtable", "DK_ORDER_RESULT_DKL")
      .option("fetchSize", "1")
      .option("user", "payadm")
      .option("password", "PAYADM")
      .load()
    jdbcDF.show()

  }
}