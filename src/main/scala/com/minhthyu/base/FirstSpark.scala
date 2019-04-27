package com.minhthyu.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FirstSpark {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("first spark word count")
//    conf.set("spark.dirver.bindAddress","127.0.0.1")
    val sc: SparkContext = SparkContext.getOrCreate(conf)
//    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("file:///Users/wy2437216707/index.html");
    val result: collection.Map[String, Long] = rdd.flatMap(line => line.split(" ")).countByValue()
    result.foreach(println)
//    Thread.sleep(100000)
    sc.stop()
  }
}
