import org.apache.spark.{SparkConf, SparkContext}

object MapOp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ds").setMaster("local")
    val context = new SparkContext(conf)
    val strings = Array("s", "sfs", "f", "d", "d")
    context.parallelize(strings, 2).map(s => s + "sub").foreach(println)
  }
}
