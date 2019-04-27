package com.minhthyu.base

import org.apache.spark.{SparkConf, SparkContext}

object RDDOperatorTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("test")
        val sc = SparkContext.getOrCreate(conf)
        //    1-100
        val seq = 1 to 100
        //    1.to(100,2).foreach(println)
        //    1-99
        //    1 until 100
        val rdd = sc.parallelize(seq)
        val rddInt = sc.parallelize(List(1,2,3,4,5,666))
        val newRDD = rdd.map(_.toString)
        val filterRDD = newRDD.filter(_.length >= 2)
        //    println(filterRDD)
        //    filterRDD.foreach(println)
        val rdd1 = sc.textFile("file/rdd.txt")
        val flatMapRDD = rdd1.flatMap(_.split(" "))
        //词频统计
        val result = flatMapRDD.countByValue()
        //    result.foreach(println)
        //    合并操作
        val unionRDD = newRDD.union(rdd1)
        //    去重
        val distinctRDD = rdd1.distinct()
        //    distinctRDD.foreach(println)
        //    交集，并且去重
        val intersectionRDD = rdd.intersection(rddInt)
        //    交集，并且不去重
        val subtractRDD = rdd.subtract(rddInt)
        //    subtractRDD.foreach(println)
        //    笛卡尔积
        val cartesianRDD = rdd.cartesian(rddInt)
        //    cartesianRDD.foreach(println)

        //     rdd.map(1 to _).foreach(println)
        //    println(rdd.flatMap(1 to _).sum)

        val collectRDD: Array[Int] = rdd.collect()
        //    collectRDD.foreach(println)
        //    first   ===
//        println(rdd.first())
        //    take  ====
        //    newRDD.take(10).foreach(println)
//        rdd.saveAsTextFile("file/rdd1")
//        rdd.saveAsObjectFile("file/rdd2")
        val fileRDD = sc.textFile("file/rdd1")
//        fileRDD.foreach(println)
        val objectRDD = sc.objectFile("file/rdd2")
//        objectRDD.foreach(println)

        val sum1 = fileRDD.map(_.toInt).reduce(_+_)
//        println(sum1)

        val sum2 = fileRDD.map(_.toInt).fold(10)(_+_)
//        println(sum2)

//        fileRDD.foreach(println)
//        fileRDD.map(_.toInt).top(5).foreach(println)
//        fileRDD.map(_.toInt).take(5).foreach(println)
//        fileRDD.map(_.toInt).takeOrdered(5).foreach(println)
//        fileRDD.map(_.toInt).takeSample(false, 1).foreach(println)

        val intRDD = fileRDD.map(_.toInt)
        val sum3 = intRDD.aggregate(0)(_+_,_+_)

        val fun1 = (x1: (Int, Int), x2:Int) => {
            (x1._1 + x2, x1._2 + 1)
        }
        val fun2 = (x1: (Int, Int), x2: (Int, Int)) => {
            (x1._1 + x2._1, x1._2 + x2._2)
        }

        var (newSum, newCount) = intRDD.aggregate((0,0))(fun1, fun2)
//        println(newSum / newCount.toDouble)



        /*
            求平均值
         */
        val tempRDD = sc.parallelize(List(1,2,3,4,5))
//        法一：
        val result1 = tempRDD.map((_, 1)).reduce((x1, x2) => {(x1._1 + x2._1, x1._2 + x2._2)})
//        println(result1)
//        法二：
        val result2 = tempRDD.map((_,1)).fold((0,0))((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2))
//        println(result2)
//        法三：
        val result3 = tempRDD.aggregate((0,0))(
            (x1, x2) => (x1._1 + x2, x1._2 + 1),
            (x1, x2) => (x1._1 + x2._1, x1._2 + x1._2))
//        println(result3)
//        println(result1._1.toDouble / result1._2)

        /*
            Pair RDD
         */
        val pair1 = rdd1.map(x => (x.split(" ")(0),x))
//        pair1.foreach(println)
        val pair2 = sc.parallelize(List((1,2),(3,4)))
        pair2.foreach(println)
        sc.stop()
    }
}
