package com.dkl.learning.reflect.method

/**
 * Created by dongkelun on 2021/4/25 14:50
 */
class Student {

  //**************成员方法***************//
  def show1(s: String): Unit = {
    System.out.println("调用了：公有的，String参数的show1(): s = " + s)
  }

  protected def show2(): Unit = {
    System.out.println("调用了：受保护的，无参的show2()")
  }

  def show3(): Unit = {
    System.out.println("调用了：默认的，无参的show3()")
  }

  private def show4(age: Int) = {
    System.out.println("调用了，私有的，并且有返回值的，int参数的show4(): age = " + age)
    "abcd"
  }

}
