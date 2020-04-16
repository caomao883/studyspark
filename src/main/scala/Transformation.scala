import org.apache.spark.{SparkConf, SparkContext}

object Transformation {
  def main(args: Array[String]): Unit = {
    //groupByKey
    //reduceByKey
    //sortByKey
    //join
    //cogroup
    flatMap
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

}
