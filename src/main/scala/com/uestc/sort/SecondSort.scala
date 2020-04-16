package com.uestc.sort

import com.uestc.sort.SecondKey
import org.apache.spark.{SparkConf, SparkContext}

object SecondSort {
  def main(args: Array[String]): Unit = {
      secondSort
  }
  def secondSort(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("secondSort")
    val sc = new SparkContext(conf)
    val path = "file:///D:/gitHub/studyspark/data/secondSort/t1.txt"
    sc.textFile(path)
      .map{line=>{
          val arrs = line.split(" ")
        val key = new SecondKey(arrs(0).toInt,arrs(1).toInt)
        (key,line)
      }}
      .sortByKey().foreach(x=>println(x._2))
  }

}
