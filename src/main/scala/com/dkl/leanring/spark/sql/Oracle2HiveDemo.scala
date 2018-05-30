package com.dkl.leanring.spark.sql
import org.apache.spark.deploy.master.Master
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object Oracle2HiveDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Oracle2HiveDemo")
      //      .master("local")
      //.config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@10.180.29.14:1521:orcl")
      .option("dbtable", "TBL_STLM_DAY_STAT")
      .option("user", "xinlian")
      .option("password", "xinlian")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
    import spark.sql
    sql("use dkl")
    df.write.mode(SaveMode.Overwrite).saveAsTable("TBL_STLM_DAY_STAT")
    df.write.mode(SaveMode.Overwrite).partitionBy("ORG_CODE").text("test")
    spark.stop
    return
    val allTablesDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@192.168.44.128:1521:orcl")
      .option("dbtable", "(select table_name,owner from all_tables where  owner  in('BIGDATA'))a")
      .option("user", "bigdata")
      .option("password", "bigdata")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .load()
    import spark.implicits._
    import spark.sql
    sql("use oracle_test")
    allTablesDF.rdd.collect().foreach(row => {
      val tableName: String = row(0).toString()
      val dataBase: String = row(1).toString()

      println(dataBase + "." + tableName)
      val df = spark.read
        .format("jdbc")
        .option("url", "jdbc:oracle:thin:@192.168.44.128:1521:orcl")
        .option("dbtable", dataBase + "." + tableName)
        .option("user", "bigdata")
        .option("password", "bigdata")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .load()
      df.write.mode("overwrite").saveAsTable(tableName)

    })

    spark.stop

  }
}