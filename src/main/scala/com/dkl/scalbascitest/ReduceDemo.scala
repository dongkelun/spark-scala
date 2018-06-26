package com.dkl.scalbascitest

object ReduceDemo {

  def main(args: Array[String]): Unit = {
    val t1 = scala.io.Source.fromFile("files/abc.txt").mkString;
    val t2 = scala.io.Source.fromFile("files/data.txt").mkString;

    val list = List(t1, t2)
    val tmp = list.flatMap(_.split(" +")).map((_, 1)).map(_._2)
    
        tmp.reduceLeft((k, v) => {
          println(k, v)
          k + v
        })
        tmp.reduce((k, v) => {
          println(k, v)
          k + v
        })
        println(tmp.reduceLeft(_ + _))
        println(tmp.reduce(_ + _))
    //

  }
}