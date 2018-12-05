package com.dkl.leanring.spark.test

import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext

    val colName = Array("ID", "TEXT_NAME", "AGE", "ADD", "IDD")
    val data = Array((1, "张三", 14, "济南", 3), (2, "张三", 14, "济南", 3))

    val rdd = sc.parallelize(data, 1)

    import spark.implicits._
    val df = spark.createDataFrame(data).toDF(colName: _*)

    df.show()

    val res = rdd.map(_ =>
      {
        val colName1 = Array("ID", "TEXT_NAME", "AGE", "ADD", "IDD")
        val data1 = Array((1, "张三", 14, "济南", 3), (2, "张三", 14, "济南", 3))
        val df1 = spark.createDataFrame(data).toDF(colName: _*)

        df1
      })
    println(res.count())

    spark.stop()
  }

}
