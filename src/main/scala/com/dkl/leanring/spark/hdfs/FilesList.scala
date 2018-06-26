package com.dkl.leanring.spark.hdfs

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
object FilesList {
  def main(args: Array[String]): Unit = {

  }

  val path = "hdfs://ambari.master.com:8020/tmp/dkl"

  val conf = new Configuration();
  val hdfs = FileSystem.get(URI.create(path), conf);
  val fs = hdfs.listStatus(new Path(path));
  val fileStatus = hdfs.getFileStatus(new Path(path));

  val modiTime = fileStatus.getModificationTime();
  val listPath = FileUtil.stat2Paths(fs);
  println(modiTime)
  listPath.foreach(println)
}