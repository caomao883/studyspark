import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {
  def main(args: Array[String]): Unit = {
    //reduce
    //collect
    //count
    //take
    countBykey
    //saveAsTextFile
    foreach
  }
  def reduce(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduce")
    val sc = new SparkContext(conf)
      .parallelize(Array(1,2,3,34),1)
      val rdd = sc.reduce(_+_)
      println(rdd)

  }
  def collect(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduce")
    val sc = new SparkContext(conf)
      .parallelize(Array(1,2,3,34),1)
    val result = sc.collect()
    for (x<-result) {
      println(x)
    }
  }
  def count(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduce")
    val sc = new SparkContext(conf)
      .parallelize(Array(1,2,3,34),1)
    val count = sc.count()
    println(count)
  }
  def take(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("reduce")
    val sc = new SparkContext(conf)
      .parallelize(Array(1,2,3,34),1)
    val arrs = sc.take(3)
    for(x<-arrs) {
      println(x)
    }
  }
  def countBykey(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("reduce")
    val sc = new SparkContext(conf)
      .parallelize(Array("aa","cc","bb","aa"),1)
    val mp = sc.map(x=>(x,1))
      .countByKey().foreach(x=>{println(x._1,x._2)})
  }
  def saveAsTextFile(): Unit = {
    val path = "file:///C:/Users/admin/Desktop/out/out1"
    val conf = new SparkConf().setMaster("local").setAppName("reduce")
    val sc = new SparkContext(conf)
      .parallelize(Array(1,2,3,34),1)
    val arrs = sc.saveAsTextFile(path)
  }
  def foreach(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("reduce")
    val sc = new SparkContext(conf)
      .parallelize(Array(1,2,3,34),1)
    val arrs = sc.foreach(x=>println(x))
  }
}
