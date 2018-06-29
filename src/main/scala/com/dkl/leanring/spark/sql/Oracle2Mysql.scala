package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession

object Oracle2Mysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDemo").master("local").getOrCreate()
    val data = Array(("1", "2", "3", "4", "5"), ("6", "7", "8", "9", "10"))
    val df = spark.createDataFrame(data).toDF("col1", "col2", "col3", "col4", "col5")

    df.show
    import org.apache.spark.sql.functions._
    def f(i: String) = {
      "newCol"
    }

    val udf1 = udf(f _)
    df.drop("col3").withColumn("col3", udf1(df("col2"))).show
    df.selectExpr("col1", "col2", "'newCol' as col3", "col4", "col5").show()
    val df1 = df.drop("col3")
    df1.show
    df.createOrReplaceTempView("table")
    df1.createOrReplaceTempView("table1")

    import spark.sql
    sql("select * from table").show
    sql("select * from table1").show

    df.drop("col3").createOrReplaceTempView("table3")
    df.drop("col3").withColumnRenamed("col2", "col3").createOrReplaceTempView("table4")
    sql("select * from table3").show
    sql("select * from table4").show
    spark.stop

  }
}