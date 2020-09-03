package com.uestc
import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{NullWritable, WritableComparable}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}

object Transformation {
  def main(args: Array[String]): Unit = {
    //groupByKey
    //reduceByKey
    //sortByKey
    //join
    //cogroup
    //flatMap
    //sequenceFile
    map_case

  }
  def groupByKey(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(Tuple2("class1",33),Tuple2("class2",22),Tuple2("class1",44),Tuple2("class3",55)))
      .groupByKey().foreach{x=>
      println(x._1)
      x._2.foreach(value=>println(value))
      println("======================================")
    }
  }
  def reduceByKey(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduceByKey")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(Tuple2("class1",33),Tuple2("class2",22),Tuple2("class1",44),Tuple2("class3",55)))
        //.reduceByKey(_+_).foreach(x=>println(x._1,x._2))
      .reduceByKey{
      (x,y)=>x+y
    }.foreach(x=>println(x))
  }
  def sortByKey(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduceByKey")
    val sc = new SparkContext(conf)
    sc.parallelize(Array(Tuple2(1,33),Tuple2(22,2),Tuple2(111,44),Tuple2(4,55)))
        .sortByKey(true).foreach(x=>println(x))
  }

  /**
  id: 1
name: Tom
socre: 90
id: 1
name: Tom
socre: 60
id: 3
name: marray
socre: 100
id: 2
name: Jack
socre: 80
    */
  def join(){
    val names = Array(Tuple2(1,"Tom"),
      Tuple2(2,"Jack"),
      Tuple2(3,"marray"))
    val scores = Array(Tuple2(1,90),
      Tuple2(2,80),
      Tuple2(3,100),
      Tuple2(1,60))
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("join")
    val sc = new SparkContext(conf)
    val namesRdd = sc.parallelize(names, 1)
    val scoresRdd = sc.parallelize(scores, 1)
    val resultRdd = namesRdd.join(scoresRdd)
    resultRdd.foreach(msg => {
      println("id: " + msg._1)
      println("name: " + msg._2._1)
      println("socre: " + msg._2._2)
    })

  }
  //与join不同,msg._2._1，msg._2._2直接是一个集合，
  /**
    * output:
    id:1
name:CompactBuffer(Tom, Tom2)
name:CompactBuffer(90, 60)
id:3
name:CompactBuffer(marray)
name:CompactBuffer(100)
id:2
name:CompactBuffer(Jack)
name:CompactBuffer(80)
    */
  def cogroup(){
    val names = Array(Tuple2(1,"Tom"),
      Tuple2(2,"Jack"),
      Tuple2(3,"marray"),Tuple2(1,"Tom2"))
    val scores = Array(Tuple2(1,90),
      Tuple2(2,80),
      Tuple2(3,100),
      Tuple2(1,60))
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("cogroup")
    val sc = new SparkContext(conf)
    val namesRdd = sc.parallelize(names, 1)
    val scoresRdd = sc.parallelize(scores, 1)
    val resultRdd = namesRdd.cogroup(scoresRdd).foreach{
      msg =>
        println("id:" + msg._1)
        println("name:" + msg._2._1)
        println("name:" + msg._2._2)

    }
//    val resultRdd = namesRdd.cogroup(scoresRdd)
//    resultRdd.foreach(msg => {
//      println("id: " + msg._1)
//      println("name: " + msg._2._1)
//      println("socre: " + msg._2._2)
//    })
  }
  def flatMap(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("groupByKey")
    val sc = new SparkContext(conf)
    sc.parallelize(Array("11:22","22:33","33:44"),1)
      .flatMap(x=>x.split(":")).foreach(x=>println(x))
  }
  def sequenceFile(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("sequenceFile")
    val sc = new SparkContext(conf)
    sc.parallelize(Array("11:22","22:33","33:44"),1).map(x=>(x,1))
    // 定义测试数据
    val studentList = List(Student("01", "abc"), Student("02", "baby"), Student("03", "xiang"))

    val path = "file:///C:/Users/admin/Desktop/out/out4"
    // 序列化测试数据到RDD,并写入到bos
    sc.parallelize(studentList)
      .repartition(1)
      // 以NullWritable 为key,构建kv结构.SequenceFile需要kv结构才能存储,NullWritable不占存储
      .map(NullWritable.get() -> _)
      // 压缩参数可选用
      .saveAsSequenceFile(s"$path")

    // 读取刚才写入的数据
    val studentRdd = sc.sequenceFile(s"$path/part-*", classOf[NullWritable], classOf[Student])
      .map {
        // 读取数据,并且重新赋值对象
        case (_, y) => Student(y.id, y.name)
      }
      .persist()

    studentRdd
      .foreach(x => println("count: " + x.id + "\t" + x.name))
  }
  def map_case(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("map+case")
    val sc = new SparkContext(conf)
    sc.parallelize(Array("11:22:33","44:dd","f")).map(line=>line.split(":"))
      .map(
        line=>if(line.length == 1) (line(0))
        else if(line.length == 2) (line(0),line(1))
        else (line(0),line(1),line(2))
      )
      .map{

        case (name,age) =>("name:"+name,"age:"+age)
        case (one) => ("one:"+one)
        case _ => ("_name","_age","_")
      }
      .foreach(println)

  }
}

case class Student(var id: String, var name: String) extends WritableComparable[Student] {
  /**
    * 重写无参构造函数,用于反序列化时的反射操作
    */
  def this() {
    this("", "")
  }

  /**
    * 继承Comparable接口需要实现的方法,用于比较两个对象的大小
    */
  override def compareTo(o: Student): Int = {
    var cmp = id compareTo o.id
    if (cmp == 0) {
      cmp = name compareTo o.name
    }
    cmp
  }

  /**
    * 继承Writable接口需要实现的方法-反序列化读取结果,并且赋值到对象字段
    * 注意要和write的顺序一致
    */
  override def readFields(in: DataInput): Unit = {
    name = in.readUTF()
    id = in.readUTF()
    println("count: " + "\t id = " + id + "\t name = " + name)
  }

  /**
    * 继承Writable接口需要实现的方法-序列化写操作,将对象字段值写入序列化
    * 注意要和readFields的顺序一致
    */
  override def write(out: DataOutput): Unit = {
    out.writeUTF(id)
    out.writeUTF(name)
  }

}
