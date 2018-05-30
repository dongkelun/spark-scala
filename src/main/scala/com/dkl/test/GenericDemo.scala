package com.dkl.test

/**
 * Scala泛型测试
 */
class A { type T }

class B[T] { def foo(p: T) { println(p) } }

class C { type T; def foo(p: T) { println(p) } }

object GenericDemo {
  def main(args: Array[String]): Unit = {
    println(new A)
    val b = new B
    println(b.asInstanceOf[AnyRef].getClass.getSimpleName)
    println(classOf[A])
    (new B).foo("hi")

    val c = new C
    val p = "hi".asInstanceOf[c.T]
    c.foo(p)

    val m = new Foo()
    println(classOf[Foo].getName)

    println(m.name)
    m.name = "oo"
    m.name_=("Foo")
    println(m.name)
  }
  def sum(a: Int, b: Int, c: Int) = a + b + c
  val b = sum(4, _: Int, 3)
  println(b(3)) //6
}

class Foo {
  def name = { "foo" }
  def name_=(str: String) {
    println("set name " + str)
  }
}
