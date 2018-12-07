package com.dkl.learning.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.Text
import org.apache.spark.sql.types.StructField
import java.util.Properties

object GZ2Oracle {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GZ2Oracle").master("local").getOrCreate()
    val sc = spark.sparkContext
    //几个hdfs文件，可以测试用
    val path1 = "hdfs://ambari.master.com:8020/tmp/dkl/20180416.txt.gz"
    val path2 = "hdfs://ambari.master.com:8020/tmp/dkl/170102.txt.gz"
    val path3 = "hdfs://ambari.master.com:8020/tmp/dkl/0801.txt.gz"
    val path4 = "hdfs://ambari.master.com:8020/tmp/dkl/20180416.txt"
    //    val data = sc.textFile(path)

    val data = transfer(sc, path4)
    //过滤
    val rdd = data.filter(_.split(" +").length == 9)
    val arr = rdd.take(2)
    //过滤掉前两行
    val rdd1 = rdd.filter(!arr.contains(_))

    //第一行为列名
    val colName = arr(0).split(" +") // +表示根据一个或多个空格进行分割

    val schema = StructType(colName.map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = rdd1.map(_.split(" +")).map(p => Row(p: _*))

    //    println(rowRDD.count)
    val df = spark.createDataFrame(rowRDD, schema)

    df.show(false)
    //    val url = "jdbc:oracle:thin:@10.180.29.14:1521:orcl"
    //    val prop = new Properties()
    //    prop.put("user", "xinlian")
    //    prop.put("password", "xinlian")
    //    prop.put("driver", "oracle.jdbc.driver.OracleDriver")
    //    df.write.mode(SaveMode.Overwrite).jdbc(url, "egaosu", prop)

    spark.stop()
    spark.close()
  }

  /**
   * 读取GBK编码格式的文件
   */
  def transfer(sc: SparkContext, path: String): RDD[String] = {
    sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], 1)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }
}
