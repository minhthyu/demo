package com.minhthyu.base

object CollectionTest {
  def main(args: Array[String]): Unit = {
    val list = List("scala", "java", "hadoop");
    list.foreach(println);
    var filterList = list.filter((str: String) => {
      str.length() > 4
    });
    //过滤
    println("====Filter====");
    filterList.foreach(println);

    var list2 = List.empty[String];

    //转化
    println("====map====");
    val newList = list.map((x:String) => {
      "^:" + x
    });
    newList.foreach(println);

    list.map((x:String) => {
      x + " : "+ x.length;
    }).foreach(println);

    //合并 聚合
    println("====reduce====")
    list.reduce(_+_).foreach(print);
    println();
    list.reduce( (x1:String, x2:String) => {
      x1 + x2;
    }).foreach(print);
  }
}