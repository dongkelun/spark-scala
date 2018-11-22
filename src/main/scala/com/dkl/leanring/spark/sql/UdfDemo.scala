package com.dkl.leanring.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Spark Sql 用户自定义函数示例
 * 博客地址：https://dongkelun.com/2018/08/02/sparkUDF/
 */
object UdfDemo {

  def main(args: Array[String]): Unit = {
    oldUdf
    newUdf
    newDfUdf
    oldDfUdf
  }

  /**
   * 根据年龄大小返回是否成年 成年：true,未成年：false
   */
  def isAdult(age: Int) = {
    if (age < 18) {
      false
    } else {
      true
    }

  }

  /**
   * 旧版本(Spark1.x)Spark Sql udf示例
   */
  def oldUdf() {

    //spark 初始化
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("oldUdf")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // 构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    //创建测试df
    val userDF = sc.parallelize(userData).toDF("name", "age")
    userDF.rdd.partitions.size
    userDF.rdd.getNumPartitions
    println(sc.defaultParallelism)
    println(userDF.except(userDF).rdd.getNumPartitions)

    // 注册一张user表
    userDF.registerTempTable("user")

    // 注册自定义函数（通过匿名函数）
    sqlContext.udf.register("strLen", (str: String) => str.length())

    sqlContext.udf.register("isAdult", isAdult _)
    // 使用自定义函数
    sqlContext.sql("select *,strLen(name)as name_len,isAdult(age) as isAdult from user").show
    //关闭
    sc.stop()
  }

  /**
   * 新版本(Spark2.x)Spark Sql udf示例
   */
  def newUdf() {
    //spark初始化
    val spark = SparkSession.builder().appName("newUdf").master("local").getOrCreate()

    // 构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))

    //创建测试df
    val userDF = spark.createDataFrame(userData).toDF("name", "age")

    // 注册一张user表
    userDF.createOrReplaceTempView("user")

    //注册自定义函数（通过匿名函数）
    spark.udf.register("strLen", (str: String) => str.length())
    //注册自定义函数（通过实名函数）
    spark.udf.register("isAdult", isAdult _)
    spark.sql("select *,strLen(name) as name_len,isAdult(age) as isAdult from user").show

    //关闭
    spark.stop()

  }

  /**
   * 新版本(Spark2.x)DataFrame udf示例
   */
  def newDfUdf() {
    val spark = SparkSession.builder().appName("newDfUdf").master("local").getOrCreate()

    // 构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))

    //创建测试df
    val userDF = spark.createDataFrame(userData).toDF("name", "age")
    import org.apache.spark.sql.functions._
    //注册自定义函数（通过匿名函数）
    val strLen = udf((str: String) => str.length())
    //注册自定义函数（通过实名函数）
    val udf_isAdult = udf(isAdult _)

    //通过withColumn添加列
    userDF.withColumn("name_len", strLen(col("name"))).withColumn("isAdult", udf_isAdult(col("age"))).show
    //通过select添加列
    userDF.select(col("*"), strLen(col("name")) as "name_len", udf_isAdult(col("age")) as "isAdult").show

    //关闭
    spark.stop()
  }
  /**
   * 旧版本(Spark1.x)DataFrame udf示例
   * 注意，这里只是用的Spark1.x创建sc的和df的语法，其中注册udf在Spark1.x也是可以使用的的
   * 但是withColumn和select方法Spark2.0.0之后才有的，关于spark1.xDataFrame怎么使用注册好的UDF没有研究
   */
  def oldDfUdf() {
    //spark 初始化
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("oldDfUdf")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // 构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    //创建测试df
    val userDF = sc.parallelize(userData).toDF("name", "age")
    import org.apache.spark.sql.functions._
    //注册自定义函数（通过匿名函数）
    val strLen = udf((str: String) => str.length())
    //注册自定义函数（通过实名函数）
    val udf_isAdult = udf(isAdult _)

    //通过withColumn添加列
    userDF.withColumn("name_len", strLen(col("name"))).withColumn("isAdult", udf_isAdult(col("age"))).show
    //通过select添加列
    userDF.select(col("*"), strLen(col("name")) as "name_len", udf_isAdult(col("age")) as "isAdult").show

    //关闭
    sc.stop()
  }

}
