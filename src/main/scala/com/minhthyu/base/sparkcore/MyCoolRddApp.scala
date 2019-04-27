package com.minhthyu.base.sparkcore

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class MyCoolRddApp {
  //  val param: Double = 3.14
  //  def rddTest(rdd: RDD[Int]): Unit = {
  //    val newParam = param
  //    val sum: Double = rdd.map(_+newParam).reduce(_+_)
  //    println(sum)
  //  }
  val rddTest = (rdd: RDD[Int], newParam : Double) => {
    val sum: Double = rdd.map(_+newParam).reduce(_+_)
    println("sum = " + sum)
  }
}

object MyCoolRddApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("test")
    conf.set("spark.driver.bindAddress","127.0.0.1")
    val sc:SparkContext = SparkContext.getOrCreate(conf)

    val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4,5))
    val my: MyCoolRddApp = new MyCoolRddApp()
    val param: Double = 3.14
    my.rddTest(rdd, param)
  }
}
