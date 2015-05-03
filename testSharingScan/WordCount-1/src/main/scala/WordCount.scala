package fr.eurecom.dsg

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Two One-by-One WordCounts")
    //conf.set("spark.scheduler.mode", "FAIR")

    val sc = new SparkContext(conf)

    val oPath1 = args(2) + "1"
    val oPath2 = args(2) + "2"

    var id : Integer = 1

    val threshold1 = args(1).toInt
    val filtered1 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold1)
    println("job" + id + " startTime: " + System.currentTimeMillis())
    filtered1.saveAsTextFile(oPath1)
    println("job" + id + " finishTime: " + System.currentTimeMillis())

    id = 2

    val threshold2 = threshold1*10
    val filtered2 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold2)
    println("job" + id + " startTime: " + System.currentTimeMillis())
    filtered2.saveAsTextFile(oPath2)
    println("job" + id + " finishTime: " + System.currentTimeMillis())
  }
}
