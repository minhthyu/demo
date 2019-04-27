package com.minhthyu.base

import scala.beans.BeanProperty

class Student {
  val name:String = "wangyu";
  var age:Int=_;
//  @BeanProperty val email:String = "2437216707@qq.com";   //产生对应的set或者get方法
  def this(age:Int) = {
    this();
    this.age = age;
  }
  def add(a:Int, b:Int):Int = {
    return a+b;
  }

  override def toString: String = {
    return "name:"+ name + " | age:" + age;
  }
}
