package com.dkl.leanring.spark.sql

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
      .setMaster("local[1]")
    conf.set("spark.cores.max", "1")
    conf.set("spark.executor.memory", "1G")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    sc.setLogLevel("WARN")
    spark_sql_bug(sqlContext, sc)
    println("completed")
    sc.stop()

  }

  def spark_sql_bug(sqlContext: HiveContext, sc: SparkContext) {
    import sqlContext.implicits._
    val r = sc.makeRDD(Seq(("440300", "440303", "1", "1000003697", "2018-04-02 23:05:14", "2018-04-03 02:40:20")))
      .toDF("f_city_id", "f_region_id", "f_device_type", "f_device_id", "f_login_time", "f_logout_time")
      .withColumn("f_home_id", lit(93L)).withColumn("f_city_id", lit(440300L)).withColumn("f_region_id", lit(440303L))
    r.registerTempTable("t_loginDF")
    sqlContext.cacheTable("t_loginDF")
    val halfHourDev: RDD[(Long, Long, String, String, String, String)] = //mappartitionsRDD才会引发，parallelcollectionRDD没问题
      //      sc.makeRDD(Seq((440300L,440303L,"1","1000003697","2018-04-02 23:05:14","2018-04-03 02:40:20")))
      //      sc.parallelize(Seq((440300L,440303L,"1","1000003697","2018-04-02 23:05:14","2018-04-03 02:40:20")),4)
      //      sqlContext.sql("SELECT f_region_id,f_city_id,cast(f_device_type as STRING) as f_device_type,f_device_id,cast(f_login_time as STRING) as f_login_time,cast(f_logout_time as STRING) as f_logout_time from t_loginDF")
      sqlContext.sql("SELECT 440303L as f_region_id,440300L as f_city_id,'1' as f_device_type,'1000003697' as f_device_id,'2018-04-02 23:05:14' as f_login_time,'2018-04-03 02:40:20' as f_logout_time from t_loginDF").rdd
        .map(x => //半小时分片统计在线设备，flatmap方便裂变
          (x.getAs[Long]("f_city_id"),
            x.getAs[Long]("f_region_id"),
            x.getAs[String]("f_device_type"),
            x.getAs[String]("f_device_id"),
            x.getAs[String]("f_login_time"),
            x.getAs[String]("f_logout_time")))
        .persist()
    halfHourDev.collect().foreach(println)
    //    sqlContext.clearCache() //clearCache无问题
    sqlContext.uncacheTable("t_loginDF") //这会导致后面查临时表出现unknown accumulator id
    halfHourDev.flatMap(v =>
      Set((23, 60))
        .map(vv => (v._1, v._2, v._3, v._4, vv._1, vv._2)))
      .toDF("f_city_id", "f_region_id", "f_device_type", "f_device_id", "f_hour", "f_timerange")
      .registerTempTable("t_half_hour_dev")
    sqlContext.cacheTable("t_half_hour_dev")
    sqlContext.sql("select * from t_half_hour_dev").show(2) //action 引发问题
    //        sqlContext.uncacheTable("t_loginDF") //这里uncacheTable无问题
    println("uncacheTable 引发 error")
  }

}
