package com.dkl.learning.spark.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.JObject
import org.apache.spark.sql.Row
import org.apache.kafka.common.serialization.StringDeserializer
import scala.math.BigInt.int2bigInt
import scala.reflect.ManifestFactory.classType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

/**
 * 此类将kafka主题中的数据按照表分别写入hive表
 *
 */
object KafkaSparkStreamingHive {
  val databaseName = "tcloud"
  val topic = "test_tcloud_all"

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("sskt").master("local[2]").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.180.29.180:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "SparkStreamingKafkaTest",
      "auto.offset.reset" -> "earliest", // latest, earliest, none
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val func = (str: String) => {
      implicit val format = DefaultFormats
      val obj = parse(str)
      val table = (obj \ "table").extract[String]

      val op_type = (obj \ "op_type").extract[String]
      val hive_deleted = if (op_type == "D") 1 else 0
      val op_ts = (obj \ "op_ts").extract[String]

      val after_obj = {
        if (op_type.equals("D")) {
          JObject((obj \ "before").asInstanceOf[JObject].obj :+
            ("hive_updated", JString(op_ts)) :+
            ("hive_deleted", JInt(hive_deleted)))
        } else {
          JObject((obj \ "after").asInstanceOf[JObject].obj :+
            ("hive_updated", JString(op_ts)) :+
            ("hive_deleted", JInt(hive_deleted)))
        }

      }
      (table, compact(render(after_obj)))
      //      Row(table, compact(render(after_obj)))
    }

    val raw_data_stream = stream.map(record => func(record.value))
    //    raw_data_stream.print()

    import spark.implicits._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import spark.sql
    //    val sss = raw_data_stream.filter(_._1.equals("TCLOUD.T_OGG2")).map(_._2)
    //    val rddd = raw_data_stream.map(_._1.distinct)
    //    sss.foreachRDD { rdd =>
    //      if (rdd.count() > 0) {
    //        val ds = spark.createDataset(rdd).asInstanceOf[Dataset[String]]
    //
    //        val table_df = spark.read.json(ds)
    //
    //        table_df.show()
    //
    //      }
    //
    //    }
    sql(s"use ${databaseName}")
    raw_data_stream.foreachRDD { rdd =>
      if (rdd.count() > 0) {
        val tables = rdd.map(_._1).collect().distinct
        tables.foreach(println)
        tables.foreach(table => {
          val rdd1 = rdd.filter(_._1.equals(table)).map(_._2)
          val ds = spark.createDataset(rdd1).asInstanceOf[Dataset[String]]

          val table_df = spark.read.json(ds)
          table_df.show()
          table_df.write.mode("append").saveAsTable(table)
        })

      }

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
