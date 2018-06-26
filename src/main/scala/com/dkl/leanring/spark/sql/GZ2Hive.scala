package com.dkl.leanring.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SaveMode

/**
 * 读取E高速压缩文件，将数据写书hive表
 * 每次执行只需通过指定参数（文件名）即可
 */
object GZ2Hive {
  def main(args: Array[String]): Unit = {
    require(args.length > 0, "必须指定文件名") //本地测试注释掉即可
    val path = args(0)

    val spark = SparkSession.builder().appName("GZ2Hive").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    //几个hdfs文件，可以测试用
    val path1 = "hdfs://ambari.master.com:8020/tmp/dkl/20180416.txt.gz"
    val path2 = "hdfs://ambari.master.com:8020/tmp/dkl/170102.txt.gz"
    val path3 = "hdfs://ambari.master.com:8020/tmp/dkl/0801.txt.gz"
    //    val data = sc.textFile(path)

    var numPartitions = 1
    if (args.length > 1)
      numPartitions = args(1).toInt
    val data = transfer(sc, path, numPartitions)
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
    spark.sql("use dkl")
    df.write.mode(SaveMode.Append).saveAsTable("t1")
    spark.stop()
  }

  /**
   * 读取GBK编码格式的文件
   */
  def transfer(sc: SparkContext, path: String, numPartitions: Int = 1): RDD[String] = {
    sc.hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], numPartitions)
      .map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
  }
}