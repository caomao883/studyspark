import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {
  def main(args: Array[String]): Unit = {
    accumulator
  }
  def accumulator(): Unit ={
    val conf = new SparkConf().setMaster("local").setAppName("accumulator")
    val sc = new SparkContext(conf)
    val sum = sc.accumulator(0)
    sc.parallelize(Array(1,2,4,5),1)
      .foreach(x=>sum.add(x))
    println(sum)

  }
}
