import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariable {
  def main(args: Array[String]): Unit = {
    broadcastVariable
  }
  def broadcastVariable(): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("broadcastVariable")
    val sc = new SparkContext(conf)

    val factor = 3
    val factorBroadcast = sc.broadcast(3)
    val rdd = sc.parallelize(Array(1,2,3,4),1)
    rdd.map{
      x=>
        x*factorBroadcast.value
    }.foreach(x=>println(x))
  }
}
