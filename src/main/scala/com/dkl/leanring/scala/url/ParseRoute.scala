package com.dkl.leanring.scala.url

import scala.io.Source
import scala.util.parsing.json.JSON
import scala.xml.XML
import java.io.PrintWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
object ParseRoute {

  val database_url = "jdbc:mysql://10.180.29.181:3306/route_analysis?useUnicode=true&characterEncoding=utf-8"
  val user = "route"
  val password = "Route-123"
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("ParseRoute").master("local").enableHiveSupport().getOrCreate()

    val df_route = spark.read
      .format("jdbc")
      .option("url", database_url)
      .option("dbtable", RoutesSql.table_route)
      .option("user", user)
      .option("password", "Route-123")
      .load()
    df_route.show()

    val df_gaode = spark.read
      .format("jdbc")
      .option("url", database_url)
      .option("dbtable", "gaode_station")
      .option("user", user)
      .option("password", "Route-123")
      .load()

    //    df_gaode.show()

    //创建临时表，便于后面用sql进行查询
    df_route.createOrReplaceTempView("route")
    df_gaode.createOrReplaceTempView("gaode")

    import spark.sql

    val df_longitudeAndLatitudes = sql(RoutesSql.longitudeAndLatitudes).cache()

    sql("use route_analysis")
    import spark.implicits._
    val stepDf = df_longitudeAndLatitudes.rdd.map(getRouteStep).flatMap(seq => {
      seq.map(step => step)
    }).toDF("stepId", "instruction", "orientation", "road", "distance", "tolls", "toll_distance", "polyline")

    stepDf.distinct().createOrReplaceTempView("stepDf")

    //将hive里的表取出来放在缓存，这样比较去重会快一点，但是要注意内存大小是否足够
    sql("select * from route_step").createOrReplaceTempView("temp_route_step")

    //将stepDf里的且hive表里没有的写到hive表里（去重）
    //    sql("select * from stepDf where stepId not in (select stepId from temp_route_step)").write.mode("append").saveAsTable("route_step")

    val polylinesDf = df_longitudeAndLatitudes.rdd.map(getRouteLine).toDF("lineId", "points", "stationId1", "stationId2", "serviceIds")
    //    polylinesDf.show()

    //写到route_line表
    //    polylinesDf.write.mode("append").saveAsTable("route_line")

    import java.sql.{ Connection, DriverManager, ResultSet };
    val conn_str = database_url + "&user=" + user + "&password=" + password
    //    val conn = DriverManager.getConnection(conn_str)
    //    try {
    //      // Configure to be Read Only
    //      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    //      // Execute Query
    //      //      val rs = statement.executeQuery("SELECT * FROM route_freq LIMIT 5")
    //      //      // Iterate Over ResultSet
    //      //      while (rs.next) {
    //      //        println(rs.getString("en_raw_name"))
    //      //      }
    //      val prep = conn.prepareStatement("update route_freq set stored=1 , stored_time=cast(CURRENT_TIMESTAMP as char) where id=?")
    //
    //      df_route.select("id").collect().foreach(row => {
    //        val id = row.getAs[Int]("id")
    //        prep.setInt(1, id)
    //        prep.executeUpdate
    //
    //      })
    //
    //    } catch {
    //      case e: Exception => e.printStackTrace
    //    } finally {
    //      conn.close
    //    }

    try {
      // Configure to be Read Only

      df_route.foreach(row => {
        val conn = DriverManager.getConnection(conn_str)
        val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        val prep = conn.prepareStatement("update route_freq set stored=1 , stored_time=cast(CURRENT_TIMESTAMP as char) where id=?")

        val id = row.getAs[Int]("id")
        prep.setInt(1, id)
        prep.executeUpdate
        conn.close()

      })

    } catch {
      case e: Exception => e.printStackTrace
    }

    spark.stop()

  }
  /**
   * 返回route_step表需要的表结构数据
   */
  def getRouteStep(row: Row) = {
    val lng1 = row.getAs[Double]("lng1")
    val lat1 = row.getAs[Double]("lat1")
    val lng2 = row.getAs[Double]("lng2")
    val lat2 = row.getAs[Double]("lat2")
    val url = s"http://restapi.amap.com/v3/direction/driving?origin=${lng1},${lat1}&destination=${lng2},${lat2}&extensions=all&output=xml&key=1c9ebb6d1d1fca0f3aa97e58f1515a45&strategy=19"
    println(url)
    //链接url，获取返回值
    val fileContent = Source.fromURL(url, "utf-8").mkString
    //写入本地磁盘
    //    println(fileContent)
    val pw = new PrintWriter("/map.xml")
    pw.write(fileContent)
    pw.flush
    pw.close
    val xmlPath = "/map.xml"

    //获取每个路线上的经纬度
    val stepInfo = getPolylineByStep(xmlPath)
    stepInfo
  }
  /**
   * 返回route_line表需要的表结构数据
   */
  def getRouteLine(row: Row) = {
    val lng1 = row.getAs[Double]("lng1")
    val lat1 = row.getAs[Double]("lat1")
    val lng2 = row.getAs[Double]("lng2")
    val lat2 = row.getAs[Double]("lat2")
    val url = s"http://restapi.amap.com/v3/direction/driving?origin=${lng1},${lat1}&destination=${lng2},${lat2}&extensions=all&output=xml&key=1c9ebb6d1d1fca0f3aa97e58f1515a45&strategy=19"
    println(url)
    //链接url，获取返回值
    val fileContent = Source.fromURL(url, "utf-8").mkString
    //写入本地磁盘
    //    println(fileContent)
    val pw = new PrintWriter("/map.xml")
    pw.write(fileContent)
    pw.flush
    pw.close
    val xmlPath = "/map.xml"

    //获取每个路线上的经纬度
    val polylines = getPolylineStepId(xmlPath).mkString(";")

    val stationId1 = row.getAs[String]("stationId1")
    val stationId2 = row.getAs[String]("stationId2")
    (stationId1 + "_" + stationId2, polylines, stationId1, stationId2, "")
  }

  def getPolylineStepId(xmlPath: String) = {
    //    val spark = SparkSession.builder().appName("xml").master("local").getOrCreate()
    val xml = XML.load(xmlPath)
    val paths = xml \\ "path" //一般会有三条路线
    val path = paths(0) // 取第一条路径（距离最短）
    val steps = path \\ "step"

    val stepInfo = steps.map(step => {
      val polyline = step \ "polyline"

      val polylineArr = polyline.text.split(";")

      val instruction = step \ "instruction"
      val orientation = step \ "orientation"
      val road = step \ "road"
      val distance = step \ "distance"
      val tolls = step \ "tolls"
      val toll_distance = step \ "toll_distance"

      //      println(instruction.text)
      //      println(polylineArr(0) + "_" + polylineArr.last)
      //      println(polyline.text)
      polylineArr(0) + "_" + polylineArr.last
    })

    stepInfo
  }

  def getPolylineByStep(xmlPath: String) = {
    //    val spark = SparkSession.builder().appName("xml").master("local").getOrCreate()
    val xml = XML.load(xmlPath)
    val paths = xml \\ "path" //一般会有三条路线
    val path = paths(0) // 取第一条路径（距离最短）
    val steps = path \\ "step"

    val stepInfo = steps.map(step => {
      val polyline = step \ "polyline"

      val polylineArr = polyline.text.split(";")

      val instruction = step \ "instruction"
      val orientation = step \ "orientation"
      val road = step \ "road"
      val distance = step \ "distance"
      val tolls = step \ "tolls"
      val toll_distance = step \ "toll_distance"

      (polylineArr(0) + "_" + polylineArr.last, instruction.text, orientation.text, road.text, distance.text, tolls.text, toll_distance.text, polyline.text)
    })

    stepInfo
  }
  /**
   * 获取每个路线上的经纬度
   */
  def getPolyline(xmlPath: String) = {
    //    val spark = SparkSession.builder().appName("xml").master("local").getOrCreate()
    val xml = XML.load(xmlPath)
    val paths = xml \\ "path" //一般会有三条路线

    //多个经纬度组成的路线
    val polylines = paths.map(path => {

      //解析每个step的第一级polyline，不解析下一级，因为和第一级是重复的
      val polyline = (path \\ "step" \ "polyline")

      //以；切分为多个经纬度组成的路线，然后去重，因为上一个路线的最后一个经纬度会和下一个路线经纬度相同
      val longitudeAndLatitudes = polyline.map(one => one.text).flatMap(_.split(";")).distinct
      longitudeAndLatitudes
    })
    polylines
  }

  /**
   * 计算每个路线相邻的两个经纬度之间的距离，并降序排序
   */
  def getPolylineDistance(arr: Seq[String]) = {

    val distance: Array[Double] = new Array(arr.length - 1)
    //key为两个经纬度 value为对应的距离
    var mapDistance: Map[String, Double] = Map()
    for (i <- 0 until distance.length) {
      val before = arr(i).split(",")
      val after = arr(i + 1).split(",")
      distance(i) = getDistance(before(0).toDouble, before(1).toDouble, after(0).toDouble, after(1).toDouble)
      mapDistance += (arr(i) + " " + arr(i + 1) -> getDistance(before(0).toDouble, before(1).toDouble, after(0).toDouble, after(1).toDouble))
    }
    import scala.collection.immutable.ListMap

    //降序排序
    val mapDistanceSort = ListMap(mapDistance.toSeq.sortWith(_._2 > _._2): _*)

    println("降序排序打印相邻经纬和距离")
    mapDistanceSort.take(5).foreach(println)
    println("降序排序打印相邻距离")
    distance.sortWith(_ > _).take(5).foreach(println)

    println("大于200米的路段所占比例")
    println(distance.filter(_ > 200).length * 1.0 / distance.length)
  }
  /**
   * 计算两位经纬之间的距离
   */
  def getDistance(longitude1: Double, latitude1: Double, longitude2: Double, latitude2: Double) = {

    val Lat1 = rad(latitude1); // 纬度
    val Lat2 = rad(latitude2);
    val a = Lat1 - Lat2; //两点纬度之差
    val b = rad(longitude1) - rad(longitude2); //经度之差
    var s = 2 * Math.asin(Math
      .sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(Lat1) * Math.cos(Lat2) * Math.pow(Math.sin(b / 2), 2))); //计算两点距离的公式
    s = s * 6378137.0; //弧长乘地球半径（半径为米）
    s = Math.round(s * 10000d) / 10000d; //精确距离的数值
    s

  }
  def rad(d: Double) = {

    d * Math.PI / 180.00; //角度转换成弧度

  }

  /**
   * 根据map的value值进行排序,ascending是否升序，默认为true，如果为false，则降序排序
   */
  def sortMapByValue(mapDistance: Map[String, Double], ascending: Boolean = true) = {
    import scala.collection.immutable.ListMap

    if (ascending) {
      //升序
      ListMap(mapDistance.toSeq.sortWith(_._2 > _._2): _*)

    } else {
      //降序
      ListMap(mapDistance.toSeq.sortWith(_._2 < _._2): _*)
    }

  }

}