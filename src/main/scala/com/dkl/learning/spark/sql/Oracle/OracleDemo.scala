package com.dkl.learning.spark.sql.Oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.util.Properties
import org.apache.spark.sql.DataFrame

object OracleDemo {
  val url = "jdbc:oracle:thin:@10.180.29.182:1521:orcl"
  val userName = "tcloud"
  val passWord = "tcloud"
  val tableName = "T_OGG4"

  def main(args: Array[String]): Unit = {
    //    append2Hive
    test {
      println("when evaluated")
      println("bb")
    }
    test1 {
      () =>
        println("when evaluated")
        println("bb")
    }
  }

  def query(): DataFrame = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", passWord)
      .load()

    jdbcDF.show()

    spark.close()
    jdbcDF
  }

  /**
   * 全字段插入
   */
  def insert() {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val colName = Array("ID", "TEXT_NAME", "AGE", "ADD", "IDD")
    val data = Array((1, "张三", 14, "济南", 3))
    import spark.implicits._
    val df = spark.createDataFrame(data).toDF(colName: _*)
    df.show
    val prop = new Properties()
    prop.put("user", userName)
    prop.put("password", passWord)
    df.write.mode(SaveMode.Append).jdbc(url, tableName, prop)
  }

  /**
   * 测试缺少列名插入
   */
  def insert1() {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val colName = Array("ID", "TEXT_NAME", "AGE", "IDD")
    val data = Array((1, "张三", 14, 3))
    import spark.implicits._
    val df = spark.createDataFrame(data).toDF(colName: _*)
    df.show
    val prop = new Properties()
    prop.put("user", userName)
    prop.put("password", passWord)
    df.write.mode(SaveMode.Append).jdbc(url, tableName, prop)
  }

  def oracle2Hive() {
    val spark = SparkSession
      .builder()
      .appName("Oracle2HiveTest")
      .master("local")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", tableName)
      .option("user", userName)
      .option("password", passWord)
      .load()
    import spark.sql
    //切换到test数据库
    sql("use dkl")
    //将df中的数据保存到hive表中（自动建表）
    df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
    //停止spark
    spark.stop

  }

  /**
   * 缺少列名插入hive
   */
  def append2Hive() {
    val spark = SparkSession
      .builder()
      .appName("Oracle2HiveTest")
      .master("local")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    val colName = Array("ID", "TEXT_NAME", "AGE", "IDD")
    val data = Array((1, "张三", 14, 1))
    val df = spark.createDataFrame(data).toDF(colName: _*)

    df.createOrReplaceTempView("temp")
    import spark.sql
    //切换到test数据库
    sql("use dkl")

    //    sql("insert into T_OGG4  select  * from temp")
    //将df中的数据保存到hive表中（自动建表）
    //    df.write.mode(SaveMode.Append).saveAsTable(tableName)
    //停止spark
    spark.stop

  }

  private[spark] def withScope[U](body: => U): U = {
    body
  }

  def test1(code: () => Unit) {
    println("start")
    code() //要想调用传入的代码块，必须写成code()，否则不会调用。
    println("end")
  }

  def test(code: => Unit) {
    println("start")
    code // 这行才会调用传入的代码块，写成code()亦可
    println("end")
  }

}
