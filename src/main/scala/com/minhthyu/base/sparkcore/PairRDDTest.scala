package com.minhthyu.base.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Student(id: String, name: String, gender: String, age:Int)

object PairRDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("PairRDDTest")
      .set("spark.driver.bindAddress","127.0.0.1")
    val sc = SparkContext.getOrCreate(conf)

    val studentRDD: RDD[String] = sc.textFile("file/rdd.txt")
    // 构建pair rdd
    val pairRDD1 = studentRDD.map(line => {
      val Array(id, name, gender, age) = line.split(" ")
      val student = new Student(id, name, gender, age.toInt)
      (id, student)
    })
//    pairRDD1.foreach(println)

    val seq = Seq((1,"java"), (2,"scala"),(3,"python"),(4,"R"))
    val pairRDD2 = sc.parallelize(seq)
//    pairRDD2.foreach(println)

    val pairRDD3 = sc.wholeTextFiles("file/rdd.txt")
//    pairRDD3.foreach(println)


//    pairRDD1.reduceByKey()

//    pairRDD1.keys.foreach(println)
//    pairRDD1.values.foreach(println)
//    pairRDD1.sortByKey().foreach(println)
//    pairRDD1.sortBy(_._2.gender).foreach(println)

    //分组
    pairRDD1.groupByKey()
    pairRDD1.groupBy(_._2.gender)


    /*
      词频统计
     */
    val line = sc.textFile("file/rdd.txt")
//    line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)
//    line.flatMap(_.split(" ")).map((_,1)).countByKey().foreach(println)


    val pets = sc.parallelize(List(("cat", 1), ("dog", 1), ("cat", 2)))
//    pets.groupByKey().foreach(println)
//    pets.reduceByKey(_+_).foreach(println)
//    pets.sortByKey().foreach(println)
//    val pets1 = sc.parallelize(List(("cat", (1,2)), ("dog", (1)), ("cat", (2))))
//    pets1.flatMapValues(_).foreach(println)
//    pets.mapValues(_*2).foreach(println)

    sc.stop()
  }
}
