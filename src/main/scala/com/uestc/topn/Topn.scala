package com.uestc.topn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求每班最高的三位分数
  */
object Topn {
  def main(args: Array[String]): Unit = {
    topn
  }
  def topn(): Unit ={
      val conf  = new SparkConf().setMaster("local").setAppName("topn")
    val path = "file:///D:/gitHub/studyspark/data/topn.txt"
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile(path)
      .map{
        line => {
          val arrs = line.split(" ")
          (arrs(0),arrs(1).toInt)
        }

      }.groupByKey()
      rdd1.flatMap{
      x =>
          val sortValues = x._2.toList.sortWith((x,y)=> x>y).take(3)
          sortValues.map(y=>(x._1,y))
      }.foreach(x=>println(x._1,x._2))
    }
}
