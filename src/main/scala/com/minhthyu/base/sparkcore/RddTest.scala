package com.minhthyu.base.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RddTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("rdd test")
    val sc:SparkContext = SparkContext.getOrCreate(conf)
    val  rdd: RDD[String] = sc.textFile("file/rdd.txt",4)
    //缓存
    //rdd.cache()
    //rdd.persist()
    rdd.persist(StorageLevel.MEMORY_ONLY)
    rdd.unpersist()
    // 分区数
    rdd.partitions.foreach(println)
    // 分区方式
    println(rdd.partitioner)
    // 内容打印
    rdd.foreach(println)
    //单词计数
    val result: collection.Map[String, Long] = rdd.flatMap(_.split(" ")).countByValue()
    result.foreach(println)
    sc.stop()
  }
}
