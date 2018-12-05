package com.dkl.blog.spark.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.collection.Seq

/**
 * Spark创建空DataFrame示例
 * 博客：https://dongkelun.com/2018/08/14/sparkEmptyDataFrame/
 */
object EmptyDataFrame {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("EmptyDataFrame").master("local").getOrCreate()

    /**
     * 创建一个空的DataFrame，代表用户
     * 有四列，分别代表ID、名字、年龄、生日
     */
    val colNames = Array("id", "name", "age", "birth")
    //为了简单起见，字段类型都为String
    val schema = StructType(colNames.map(fieldName => StructField(fieldName, StringType, true)))
    //主要是利用了spark.sparkContext.emptyRDD
    val emptyDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

    emptyDf.show

    /**
     * 也可以给每列指定相对应的类型
     */
    val schema1 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("birth", StringType, true)))
    val emptyDf1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
    emptyDf1.show

    //还有一种空的DataFrame，没有任何行任何列
    spark.emptyDataFrame.show

    spark.stop()
  }

}
