package com.dkl.blog.spark.hive

import org.apache.spark.sql.SparkSession
import scala.reflect.api.materializeTypeTag

/**
 * 新版本spark-hive测试
 * 博客：Spark连接Hive（spark-shell和Eclipse两种方式）
 * https://dongkelun.com/2018/03/25/sparkHive/
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
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    val data = Array((1, "val1"), (2, "val2"), (3, "val3"))
    var df = spark.createDataFrame(data).toDF("key", "value")
    df.createOrReplaceTempView("temp_src")
    sql("insert into src select key,value from temp_src")
    sql("SELECT * FROM src").show()
  }
}
