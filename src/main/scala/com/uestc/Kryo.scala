package com.uestc

import com.yammer.metrics.core.Counter
import org.apache.spark.{SparkConf, SparkContext}

object Kryo {
  def main(args: Array[String]): Unit = {

  }

  /**
    * 序列化
    */
  def kryoSeriable(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("seriable")
    conf.registerKryoClasses(Array(classOf[Counter]))
    val sc = new SparkContext(conf)
    sc.parallelize(Array(1,2,3,4))

  }

  /**
    * 反序列化
    */

  def ArvoSeriable(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("seriable")
    conf.registerAvroSchemas(Array(classOf[Counter]))
    val sc = new SparkContext(conf)
    sc.parallelize(Array(1,2,3,4))

  }

}
