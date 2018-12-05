package com.dkl.blog.hdfs

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileUtil
import scala.collection.mutable.ArrayBuffer

/**
 * 主要目的是打印某个hdfs目录下所有的文件名，包括子目录下的
 * 其他的方法只是顺带示例，一边有其它需求可以参照改写
 *
 * 博客：打印（获取）HDFS路径下所有的文件名（包括子目录下的）
 * https://dongkelun.com/2018/11/20/getAllHDFSFileNames/
 */
object FilesList {

  def main(args: Array[String]): Unit = {
    val path = "hdfs://ambari.master.com:8020/tmp/dkl"

    println("打印所有的文件名，包括子目录")
    listAllFiles(path)
    println("打印一级文件名")
    listFiles(path)
    println("打印一级目录名")
    listDirs(path)
    println("打印一级文件名和目录名")
    listFilesAndDirs(path)
    //    getAllFiles(path).foreach(println)
    //    getFiles(path).foreach(println)
    //    getDirs(path).foreach(println)
  }

  def getHdfs(path: String) = {
    val conf = new Configuration()
    FileSystem.get(URI.create(path), conf)
  }
  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  /**************直接打印************/

  /**
   * 打印所有的文件名，包括子目录
   */
  def listAllFiles(path: String) {
    val hdfs = getHdfs(path)
    val listPath = getFilesAndDirs(path)
    listPath.foreach(path => {
      if (hdfs.getFileStatus(path).isFile())
        println(path)
      else {
        listAllFiles(path.toString())

      }
    })
  }
  /**
   * 打印一级文件名
   */
  def listFiles(path: String) {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile()).foreach(println)
  }

  /**
   * 打印一级目录名
   */
  def listDirs(path: String) {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory()).foreach(println)
  }
  /**
   * 打印一级文件名和目录名
   */
  def listFilesAndDirs(path: String) {
    getFilesAndDirs(path).foreach(println)
  }

  /**************直接打印************/

  /**************返回数组************/
  def getAllFiles(path: String): ArrayBuffer[Path] = {
    val arr = ArrayBuffer[Path]()
    val hdfs = getHdfs(path)
    val listPath = getFilesAndDirs(path)
    listPath.foreach(path => {
      if (hdfs.getFileStatus(path).isFile()) {
        arr += path
      } else {
        arr ++= getAllFiles(path.toString())
      }

    })
    arr
  }
  def getFiles(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile())
  }
  def getDirs(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory())
  }
  /**************返回数组************/

}
