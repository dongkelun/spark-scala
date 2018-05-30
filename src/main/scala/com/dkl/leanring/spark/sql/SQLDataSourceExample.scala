package com.dkl.leanring.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object SQLDataSourceExample {
  case class Person(name: String, age: Long)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SQLDataSourceExample")
      .master("local")
      .getOrCreate()

    runBasicDataSourceExample(spark)
  }

  private def runBasicDataSourceExample(spark: SparkSession): Unit = {
    // $example on:generic_load_save_functions$
    val usersDF = spark.read.load("src/main/resources/scala/users.parquet")
    usersDF.show()
    usersDF.select("name", "favorite_color").write.mode(SaveMode.Overwrite).save("namesAndFavColors.parquet")

    val peopleDF = spark.read.format("json").load("src/main/resources/scala/people.json")
    peopleDF.show()
    peopleDF.select("name", "age").write.format("parquet").mode(SaveMode.Overwrite).save("namesAndAges.parquet")

    //直接在文件上运行sql
    val sqlDF = spark.sql("SELECT * FROM parquet.`src/main/resources/scala/users.parquet`")
    sqlDF.show()
  }
}