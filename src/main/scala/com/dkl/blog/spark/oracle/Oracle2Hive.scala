package com.dkl.blog.spark.oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

/**
 *
 * Spark自动建表（带字段注释、暂无表注释）
 * 并将中间库Oracle的历史数据全部初始化到hive
 *
 * 实现方案：
 * 1、 利用Spark的自动字段类型匹配
 * 2、 读取Oracle表字段注释，添加到DataFrame的元数据
 * 3、按需修改Spark默认的字段类型转换
 *
 * 注：需要提前建好对应的hive数据库
 *
 * 博客：利用Spark实现Oracle到Hive的历史数据同步
 *
 * https://dongkelun.com/2018/08/27/sparkOracle2Hive/
 */
object Oracle2Hive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Oracle2Hive")
      .master("local")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    //oracle的连接信息
    val p = new Properties()
    p.put("driver", "oracle.jdbc.driver.OracleDriver")
    p.put("url", "jdbc:oracle:thin:@192.168.44.128:1521:orcl")
    p.put("user", "bigdata")
    p.put("password", "bigdata")

    import scala.collection.JavaConversions._
    val database_conf: scala.collection.mutable.Map[String, String] = p

    //Oracle是分用户的，这里以用户BIGDATA为例
    val owner = "BIGDATA"
    val sql_in_owner = s"('${owner}')"

    database_conf.put("dbtable", "TEST")

    spark.sql(s"use ${owner}")

    database_conf.put("dbtable", s"(select table_name from all_tables where owner in ${sql_in_owner})a")
    //所有的表名
    val allTableNames = getDataFrame(spark, database_conf)

    database_conf.put("dbtable", s"(select * from all_col_comments where owner in ${sql_in_owner})a")
    //所有的表字段对应的注释
    val allColComments = getDataFrame(spark, database_conf).repartition(160).cache

    allTableNames.select("table_name").collect().foreach(row => {
      //表名
      val table_name = row.getAs[String]("table_name")
      database_conf.put("dbtable", table_name)
      //根据表名从Oracle取数
      val df = getDataFrame(spark, database_conf)
      //字段名 和注 对应的map
      val colName_comments_map = allColComments.where(s"TABLE_NAME='${table_name}'")
        .select("COLUMN_NAME", "COMMENTS")
        .na.fill("", Array("COMMENTS"))
        .rdd.map(row => (row.getAs[String]("COLUMN_NAME"), row.getAs[String]("COMMENTS")))
        .collect()
        .toMap

      val colName = ArrayBuffer[String]()
      //为schema添加注释信息
      val schema = df.schema.map(s => {
        if (s.dataType.equals(DecimalType(38, 0))) {
          colName += s.name
          new StructField(s.name, IntegerType, s.nullable, s.metadata).withComment(colName_comments_map(s.name))
        } else {
          s.withComment(colName_comments_map(s.name))
        }
      })

      import org.apache.spark.sql.functions._
      var df_int = df
      colName.foreach(name => {
        df_int = df_int.withColumn(name, col(name).cast(IntegerType))

      })
      //根据添加了注释的schema，新建DataFrame
      val new_df = spark.createDataFrame(df_int.rdd, StructType(schema))
      new_df.write.mode("overwrite").saveAsTable(table_name)

      //      new_df.schema.foreach(s => println(s.metadata))
      //      new_df.printSchema()

    })

    spark.stop

  }
  /**
   * @param spark SparkSession
   * @param database_conf 数据库配置项Map，包括driver,url,username,password,dbtable等内容，提交程序时需用--jars选项引用相关jar包
   * @return 返回DataFrame对象
   */
  def getDataFrame(spark: SparkSession, database_conf: scala.collection.Map[String, String]) = {
    spark.read.format("jdbc").options(database_conf).load()
  }

}
