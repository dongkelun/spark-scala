package com.dkl.learning.spark.sql

import org.apache.spark.sql.SparkSession

object ReadDemo {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ReadDemo").master("local").getOrCreate()

    runDatasetCreationExample(spark)
    runBasicDataFrameExample(spark)

  }

  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    import spark.implicits._
    // $example on:create_ds$
    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "src/main/resources/scala/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

  }
  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    // Encoders are created for case classes

    val df = spark.read.json("src/main/resources/scala/people.json")
    df.show()
    df.printSchema()

    import spark.implicits._
    // Select everybody, but increment the age by 1
    df.select("name").show()

    df.select($"name", $"age" + 1).show()

    // Select people older than 21
    df.filter($"age" > 21).show()

    // 和filter等价
    //df.where("age<21").show()

    df.createOrReplaceTempView("people")

    import spark.sql
    val sqlDF = sql("SELECT * FROM people")
    sqlDF.show()
  }
}
