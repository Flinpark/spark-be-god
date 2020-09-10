package org.flinpark.spark.operation

import org.apache.spark.{SparkConf, SparkContext}

object DisDataset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dataset").setMaster("local")
    val context = new SparkContext(conf)
    //创建Dataset
    val ints = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    //并行化输出，第二个参数指定分区数
    println(context.parallelize(ints, 3).mapPartitionsWithIndex((i,it)=>{
      println(s"current partition :${i}")
//      println("value is :"+it.foreach(print))
      /**迭代器遍历完数据就没了
       * 放开上面一会会有如下异常：
       * Exception in thread "main" java.lang.UnsupportedOperationException: empty collection
       * at org.apache.spark.rdd.RDD.$anonfun$reduce$4(RDD.scala:1096)
       * at scala.Option.getOrElse(Option.scala:189)
       * at org.apache.spark.rdd.RDD.$anonfun$reduce$1(RDD.scala:1096)
       * at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
       * at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
       * at org.apache.spark.rdd.RDD.withScope(RDD.scala:388)
       * at org.apache.spark.rdd.RDD.reduce(RDD.scala:1076)
       * at org.flinpark.spark.operation.DisDataset$.main(DisDataset.scala:16)
       * at org.flinpark.spark.operation.DisDataset.main(DisDataset.scala)
       */
      it
    }).reduce((a, b) => a + b))
  }
}
