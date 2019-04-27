package com.minhthyu.base

import scala.beans.BeanProperty

class Teacher(name:String = "wangmin",
              var age:Int=0,
              val phone:String,
              @BeanProperty val email:String = ""){
}
