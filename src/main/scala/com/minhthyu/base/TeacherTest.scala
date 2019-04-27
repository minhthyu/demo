package com.minhthyu.base

object TeacherTest {
  def main(args: Array[String]): Unit = {
    var t1 = new Teacher("wangmin", phone = "123");
    var t2 = new Teacher( name = "wm", phone = "123", age = 21);
    var t3 = new Teacher( name = "wm", phone = "123", email = "12@qq.com");
  }
}
