package com.dkl.leanring.spark.sql.Oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * Spark通过修改DataFrame的schema给表字段添加注释
 * 博客：https://dongkelun.com/2018/08/20/sparkDfAddComments/
 */
object OracleSchemaDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OracleSchemaDemo").master("local").enableHiveSupport().getOrCreate()
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@192.168.44.128:1521:orcl")
      .option("dbtable", "ORA_TEST")
      .option("user", "bigdata")
      .option("password", "bigdata")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
    df.schema.foreach(s => println(s.name, s.metadata))

    val commentMap = Map("ID" -> "ID", "NAME" -> "名字")

    val schema = df.schema.map(s => {
      s.withComment(commentMap(s.name))
    })

    //根据添加了注释的schema，新建DataFrame
    val new_df = spark.createDataFrame(df.rdd, StructType(schema)).repartition(160)

    new_df.schema.foreach(s => println(s.name, s.metadata))

    spark.sql("use test")
    //保存到hive
    new_df.write.mode("overwrite").saveAsTable("ORA_TEST")

    spark.stop

  }
}
