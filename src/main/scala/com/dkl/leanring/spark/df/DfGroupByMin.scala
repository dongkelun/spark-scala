package com.dkl.leanring.spark.df

import org.apache.spark.sql.SparkSession

object DfGroupByMin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DfGroupByMin").master("local").getOrCreate()
    val data = Array((1, 2, 3), (1, 8, 6), (1, 2, 3), (4, 5, 9), (4, 3, 4), (4, 2, 1))
    val df = spark.createDataFrame(data).toDF("id", "value", "other")
    df.union(df).show
    df.createOrReplaceTempView("table")

    import spark.sql

    sql("select id,min(value) as min_value from table group by id").createOrReplaceTempView("df1")

    sql("select table.* from table,df1 where table.id=df1.id and value=min_value").show

    println(sql("select table.* from table,df1 where table.id=df1.id and value=min_value").rdd.getNumPartitions)

    df.show

    spark.stop()

  }
}
