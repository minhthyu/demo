package com.minhthyu.base

object StudentTest {
  def main(args: Array[String]): Unit = {
    val stu1 = new Student();
    val stu2 = new Student(18);
    println(stu1);
    println(stu2);
    println(stu2.age);//取值方法
    stu1.age = 20;    //赋值
    println(stu1.age)
    stu1.age_=(22);   //赋值
    println(stu1.age);
//    println(stu1.getEmail);
  }
}
