import org.apache.spark.{SparkConf, SparkContext}

object StudySpark {
  def main(args: Array[String]): Unit = {
    groupByKey
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
}
