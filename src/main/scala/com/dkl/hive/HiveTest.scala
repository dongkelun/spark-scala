package com.dkl.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import java.text.SimpleDateFormat
import java.util.Date

object HiveTest {

  def main(args: Array[String]): Unit = {

    println(NowDate)
    def NowDate(): String = {
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = dateFormat.format(now)
      return date
    }

    //    val map1 = Map("key1" -> 1, "key2" -> 3, "key3" -> 5)
    //    println(map1.keySet)
    //    println(map1.keys.mkString(" "))

    val spark = SparkSession
      .builder()
      .appName("SparkHive")
      .master("local")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    val str = "\"\"\"\"310000\"\""

    println(str)
    println(str.replace("\"\"", "\""))
    val df = spark.table("dkl.test_partition")
    println(df.limit(0).rdd.isEmpty())
    println(df.limit(1).count)
    df.groupBy().count
    println(df.limit(1).rdd.partitions.isEmpty)
    val schema1 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("birth", StringType, true)))
    val emptyDf1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)

    println(emptyDf1.rdd.isEmpty())
    println(emptyDf1.rdd.partitions.length)
    println(df.limit(0).rdd.partitions.length)
    df.limit(0).show
    println(emptyDf1.limit(0).count)
    emptyDf1.show
    //    spark.sql("select * from dkl.test_partition").show

    spark.stop
  }
}
