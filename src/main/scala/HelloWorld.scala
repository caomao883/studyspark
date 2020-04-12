import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val path = "file:///C:/Users/admin/Desktop/out/out1"
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("HelloWorld")
    val sc = new SparkContext(conf)
    val arr = Array(1,2,3,4)
    sc.parallelize(arr,1)
    .saveAsTextFile(path)
  }
}
