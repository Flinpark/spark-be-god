package org.flinpark.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
    val context = new SparkContext(conf)
    val file = context.textFile("file:///Users/admin/Desktop/Work/SQL/prmethues_test_sql.sql")
    file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a,b) => a+b).foreach(println)
  }

}
