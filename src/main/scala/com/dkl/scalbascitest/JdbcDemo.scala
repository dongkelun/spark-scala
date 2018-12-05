package com.dkl.scalbascitest
import java.sql.{ Connection, DriverManager, ResultSet };

/**
 * Scala JDBC 连接 MySql
 */
object JdbcDemo {

  def main(args: Array[String]): Unit = {
    val conn_str = "jdbc:mysql://10.180.29.181:3306/route_analysis?useUnicode=true&characterEncoding=utf-8&user=route&password=Route-123"

    val database_url = "jdbc:mysql://10.180.29.181:3306/route_analysis?useUnicode=true&characterEncoding=utf-8"
    val user = "route"
    val password = "Route-123"
    // Load the driver
    val conn = DriverManager.getConnection(conn_str)
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      // Execute Query
      //      val rs = statement.executeQuery("SELECT * FROM route_freq LIMIT 5")
      //      // Iterate Over ResultSet
      //      while (rs.next) {
      //        println(rs.getString("en_raw_name"))
      //      }
      val prep = conn.prepareStatement("update route_freq set freq=? where id=?")
      prep.setInt(1, 3)
      prep.setInt(2, 1)
      prep.executeUpdate
    } catch {
      case e: Exception => e.printStackTrace
    } finally {
      conn.close
    }

  }
}
