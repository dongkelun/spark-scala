package com.dkl.blog.spark.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

/**
 * 博客：Spark 将DataFrame所有的列类型改为double
 * https://dongkelun.com/2018/04/27/dfChangeAllColDatatypes/
 *
 */
object ChangeAllColDatatypes {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ChangeAllColDatatypes").master("local").getOrCreate()
    import org.apache.spark.sql.types._
    val data = Array(("1", "2", "3", "4", "5"), ("6", "7", "8", "9", "10"))
    val df = spark.createDataFrame(data).toDF("col1", "col2", "col3", "col4", "col5")

    import org.apache.spark.sql.functions._
    df.select(col("col1").cast(DoubleType)).show()

    val colNames = df.columns

    var df1 = df
    for (colName <- colNames) {
      df1 = df1.withColumn(colName, col(colName).cast(DoubleType))
    }
    df1.show()

    val cols = colNames.map(f => col(f).cast(DoubleType))
    df.select(cols: _*).show()
    val name = "col1,col3,col5"
    df.select(name.split(",").map(name => col(name)): _*).show()
    df.select(name.split(",").map(name => col(name).cast(DoubleType)): _*).show()

    spark.stop
  }
}
