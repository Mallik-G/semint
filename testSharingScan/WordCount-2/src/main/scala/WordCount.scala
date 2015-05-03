package fr.eurecom.dsg

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object WordCount {
  
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Two Concurrent WordCounts - No Caching")
    conf.set("spark.scheduler.mode", "FAIR")

    val sc = new SparkContext(conf)

    val oPath1 = args(2) + "1"
    val oPath2 = args(2) + "2"

    val threshold1 = args(1).toInt
    val filtered1 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold1)

    val threshold2 = threshold1*10
    val filtered2 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold2)

    val job1 : JobConcurent = new JobConcurent(filtered1, 1, oPath1)
    val job2 : JobConcurent = new JobConcurent(filtered2, 2, oPath2)
    job1.start()
    job2.start()    
  }
}

class JobConcurent(rdd: RDD[_], id: Integer, output: String) extends Thread {
  override def run(): Unit = {
    println("running job" + id)
    println("job" + id + " startTime: " + System.currentTimeMillis())
    rdd.saveAsTextFile(output)
    println("job" + id + " finishTime: " + System.currentTimeMillis())
  }
}
