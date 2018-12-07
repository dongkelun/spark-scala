package com.dkl.learning.spark.df

import org.apache.spark.sql.SparkSession

object NotInDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DfSortDesc").master("local").getOrCreate()

    val data = Array((1, 2, 3), (2, 8, 6), (3, 5, 9))
    val df = spark.createDataFrame(data).toDF("col1", "col2", "col3")

    //    val dff = df.limit(2)
    //    dff.show
    //    df.show()
    //    df.except(dff).show
    val data1 = Array((1, 2), (3, 4))
    import org.apache.spark.sql.types._
    val df1 = spark.createDataFrame(data1).toDF("col1", "col2")
    val col1 = df.select("col1").except(df1.select("col1"))
    //      .rdd.map(row => row.getAs[Int]("col1")).collect()
    import org.apache.spark.sql.functions._
    df.where(col("col1").isin(col1("col1"))).show

    //    val df2 = spark.createDataFrame(data1).toDF("col1", "col2")
    //    df1.union(df2).show
    //    df1.select("col1").createOrReplaceTempView("temp")
    //
    //    df.show
    //    df.where(s"col1 not in (select col1 from temp)").show
    //    df1.show
    df1("col1").contains("")
    //    df.filter(row => {
    //      val col1 = row.getAs[Int]("col1");
    //      df1("col1").contains(col1)
    //    })

    spark.stop

  }

}
