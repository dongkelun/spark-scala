package com.dkl.leanring.spark.kafka

import scala.util.parsing.json.JSON
import scala.collection.immutable.HashMap
object KafkaJson {

  val ss = """{"table":"XINLIAN.TBL_CM_MEDIA_RES","op_type":"U","op_ts":"2018-06-07 16:46:32.940492",
"current_ts":"2018-06-07T16:46:40.499000","pos":"00000000220093626802",
"primary_keys":["VERSION","MED_SEQ","MED_PATH","MED_DIR","MED_TYPE","MED_NAME","MED_DESC","MIME_TYPE",
"CUSTOMER_CODE","MED_INFO","REC_CRT_TIME","REC_CRT_ACC","MID","REF_NO"],"before":{"VERSION":0,"MED_SEQ":2318,
"MED_PATH":"/attach/201703/1b01a820-af96-4b92-a54a-4ab5a78bb013/1.jpg",
"MED_DIR":"/attach/201703/1b01a820-af96-4b92-a54a-4ab5a78bb013/",
"MED_TYPE":null,"MED_NAME":"1.jpg","MED_DESC":null,"MIME_TYPE":"image/jpeg",
"CUSTOMER_CODE":510,"MED_INFO":"testogg","REC_CRT_TIME":"20170328142507",
"REC_CRT_ACC":"510","MID":null,"REF_NO":"000000000001219"},
"after":{"VERSION":0,"MED_SEQ":2318,
"MED_PATH":"/attach/201703/1b01a820-af96-4b92-a54a-4ab5a78bb013/1.jpg",
"MED_DIR":"/attach/201703/1b01a820-af96-4b92-a54a-4ab5a78bb013/",
"MED_TYPE":null,"MED_NAME":"1.jpg","MED_DESC":null,
"MIME_TYPE":"image/jpeg","CUSTOMER_CODE":510,
"MED_INFO":"test","REC_CRT_TIME":"20170328142507",
"REC_CRT_ACC":"510","MID":null,"REF_NO":"000000000001219"}}
"""

  def main(args: Array[String]): Unit = {
    println(ss.length())

    val b = JSON.parseFull(ss)

    b match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(map: Map[String, Any] @unchecked) =>
        val before = map("before").asInstanceOf[Map[String, Any]]
        val key = before.keys.toSeq
        println(key(0), before(key(0)))

        println(map.keys, map("before").asInstanceOf[Map[String, Any]].keys)
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }

  }

}