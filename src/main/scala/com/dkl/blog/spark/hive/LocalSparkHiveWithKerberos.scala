package com.dkl.blog.spark.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession

/**
 * Created by dongkelun on 2021/5/18 19:29
 *
 * Spark 本地连接远程服务器上带有kerberos认证的Hive
 */
object LocalSparkHiveWithKerberos {

  def main(args: Array[String]): Unit = {

    try {

      //等同于把krb5.conf放在$JAVA_HOME\jre\lib\security，一般写代码即可
      System.setProperty("java.security.krb5.conf", "D:\\conf\\inspur\\krb5.conf")

      //下面的conf可以注释掉是因为在core-site.xml里有相关的配置，如果没有相关的配置，则下面的代码是必须的
      //      val conf = new Configuration
      //      conf.set("hadoop.security.authentication", "kerberos")
      //      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab("hive/indata-192-168-44-128.indata.com@INDATA.COM", "D:\\conf\\inspur\\hive.service.keytab")
      println(UserGroupInformation.getCurrentUser, UserGroupInformation.getLoginUser)


    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("LocalSparkHiveWithKerberos")
      //      .config("spark.kerberos.keytab", "hive/indata-192-168-44-128.indata.com@INDATA.COM")
      //      .config("spark.kerberos.principal", "D:\\conf\\inspur\\hive.service.keytab")
      .enableHiveSupport()
      .getOrCreate()

    spark.table("sjtt.trafficbase_cljbxx").show()

    spark.stop()
  }
}
