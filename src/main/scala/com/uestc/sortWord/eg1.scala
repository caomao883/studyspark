package com.uestc.sortWord

import org.apache.spark.{SparkConf, SparkContext}

/**
  *按单词出现的次数从多到少排序
  */
object eg1 {
  def main(args: Array[String]): Unit = {
    func
  }
  def func(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("func")
    val sc = new SparkContext(conf)
    sc.parallelize(Array("aa","bb","cc","ee","bb","bb","ee"),1)
      .map(x=>(x,1))
      .reduceByKey(_+_)
        .map(x=>(x._2,x._1))
      .sortByKey(false).foreach(x=>println(x._2,x._1))
  }
}
