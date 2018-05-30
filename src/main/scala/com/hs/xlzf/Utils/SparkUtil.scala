package com.hs.xlzf.Utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

object SparkUtil {
  /**
   * @param appName 应用名称
   * @return sc SparkContext
   */
  def init_sc(appName: String = "defaut_app") = {
    val app_name = appName + "_" + System.currentTimeMillis()
    val conf = new SparkConf().setAppName(app_name).setMaster("local")
    //    val conf = new SparkConf().setAppName(app_name).setMaster("spark://192.168.44.128:7077")
    new SparkContext(conf)
  }

  //  /**
  //   * @param appName 应用名称
  //   * @return spark SparkSession
  //   */
  //  def init_spark(appName: String = "defaut_app") = {
  //    val app_name = appName + "_" + System.currentTimeMillis()
  //    SparkSession.builder().appName(app_name).master("local").config("spark.debug.maxToStringFields", 1000).getOrCreate()
  //  }

  /**
   * @param appName 应用名称
   * @return spark SparkSession
   */
  def init_spark(appName: String = "defaut_app", masterName: String = "local") = {
    val app_name = appName + "_" + System.currentTimeMillis()
    SparkSession.builder().appName(app_name).master(masterName).config("spark.debug.maxToStringFields", 1000).getOrCreate()
  }

  /**
   * @param sc SparkContext
   * @param database_conf 数据库配置项Map，包括driver,url,username,password,dbtable等内容，提交程序时需用--jars选项引用相关jar包
   * @return 返回DataFrame对象
   */
  def getDataFrame(sc: SparkContext, database_conf: scala.collection.Map[String, String]) = {
    val sqlContext = new SQLContext(sc)
    sqlContext.read.format("jdbc").options(database_conf).load()
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
