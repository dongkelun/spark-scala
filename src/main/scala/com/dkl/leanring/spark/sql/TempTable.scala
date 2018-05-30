package com.dkl.leanring.spark.sql

import com.hs.xlzf.Utils.SparkUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object TempTable {

  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.init_sc("TempTable");
    val spark = SparkUtil.init_spark("TempTable")
    val sqlContext = new SQLContext(sc)
    val hiveCtx = new HiveContext(sc)
    //    hiveCtx.sql("use default")
    hiveCtx.sql("select * from test1").show()
    import sqlContext.implicits._

    val data = Array((3, "name3"), (4, "name4"), (5, "name5"))
    val scdf = sc.parallelize(data).toDF("id", "name")
    scdf.registerTempTable("USER_SC")
    hiveCtx.sql("insert into test1 select id,name from USER_SC")
    hiveCtx.sql("select * from test1").show()
    sqlContext.sql("SELECT name FROM USER_SC").show()

    var df = spark.createDataFrame(data).toDF("id", "name")
    df.createTempView("USER")
    val sqlDF = spark.sql("SELECT id FROM USER")
    sqlDF.show()

  }
}