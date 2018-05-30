package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * 新版本spark-hive测试
 */
object NewSparkHiveDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    //    sql("use oracle_test")
    //    sql("CREATE TABLE IF NOT EXISTS src1 (key INT, value STRING)")
    //    val data = Array((1, "val1"), (2, "val2"), (3, "val3"))
    //    var df = spark.createDataFrame(data).toDF("key", "value")
    //    df.createOrReplaceTempView("temp_src")
    //    sql("insert into src1 select key,value from temp_src")
    //    sql("SELECT * FROM src1").show()
    sql("use dkl")
    sql("select * from t1").show()
    sql("select count(1) from t1").show
    spark.stop
  }
}