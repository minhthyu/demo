//package com.minhthyu.base
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//object CountRDDLength {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf()
//    conf.setMaster("local[*]")
//    conf.setAppName("CountRDDLength")
//    conf.set("spark.driver.bindAddress","127.0.0.1")
//    val sc: SparkContext = SparkContext.getOrCreate(conf)
//    val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4,5))
//    println(rdd.getNumPartitions)
//    var count: Int = 0
//    rdd.foreach( x => {
//      count += 1
//      println("rdd foreach中的元素个数为：" + count)
//    })
//    println("rdd中的元素个数为：" + count)
//  }
//}
//
////object CountListLength {
////  def main(args: Array[String]): Unit = {
////    val conf: SparkConf = new SparkConf()
////    conf.setMaster("local[*]")
////    conf.setAppName("CountListLength")
////    val sc: SparkContext = SparkContext.getOrCreate(conf)
////    val rdd: RDD[Int] = sc.parallelize(List(1,2,3,4,5))
////    var i = 0
////    val it = rdd.collect().iterator
////    while (it.hasNext == true){
////      it.next()
////      i += 1
////    }
////    println("rdd的长度为：" + i)
////  }
////}


package com.base.sparkAdvence

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRank {
  def main(args: Array[String]): Unit = {
    //1.构建SparkConf对象
    val conf = new SparkConf
    conf.setAppName("网页排名").setMaster("local[*]")
    conf.set("spark.driver.bindAddress","127.0.0.1")
    //2.构建SparkContext对象
    val sc = SparkContext.getOrCreate(conf)

    //计算网页排名
    //网页链接数据集
    val links=Array(("A",List("B","C"))
      ,("B",List("C"))
      ,("C",List("D"))
      ,("D",List("A","C"))
    )
    val linksRDD=sc.parallelize(links)
      //优化操作，预定定义分区
      .partitionBy(new HashPartitioner(3)).cache();

    //页面权重
    val ranks=List("A"->1.0,"B"->1.0,"C"->1.0,"D"->1.0);
    var ranksRDD=sc.parallelize(ranks);


    linksRDD.join(ranksRDD).map(x=>{
      val page = x._1
      val link = x._2._1
      val rank = x._2._2
      val avg = rank/link.size
      link.map(p=>{
        (p, avg)
      })
    }).foreach(println)

    linksRDD.join(ranksRDD).flatMap(x=>{
      val page = x._1
      val link = x._2._1
      val rank = x._2._2
      val avg = rank/link.size
      link.map(p=>{
        (p, avg)
      })
    }).foreach(println)

    //迭代多次，计算权重值
    val iterator=50;
    (1 to iterator).foreach(x=>{
      //更新权重值 =权重值/链接个数
      ranksRDD=linksRDD.join(ranksRDD).flatMap(x=>{
        val page=x._1;
        val link=x._2._1;
        val rank=x._2._2;
        val avg=rank/link.size
        link.map(p=>{
          (p,avg)
        })
      }).reduceByKey(_+_).mapValues(x=>x*0.85+0.15)
    })
    ranksRDD.foreach(println)

    sc.stop();
  }
}
