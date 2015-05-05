package fr.eurecom.dsg

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Ten Concurrent WordCounts - Caching")
    conf.set("spark.scheduler.mode", "FAIR")

    val sc = new SparkContext(conf)

    val oPath1 = args(2) + "1"
    val oPath2 = args(2) + "2"
    val oPath3 = args(2) + "3"
    val oPath4 = args(2) + "4"
    val oPath5 = args(2) + "5"
    val oPath6 = args(2) + "6"
    val oPath7 = args(2) + "7"
    val oPath8 = args(2) + "8"
    val oPath9 = args(2) + "9"
    val oPath10 = args(2) + "10"

    val threshold1 = args(1).toInt
    val filtered1 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold1)

    val threshold2 = args(1).toInt
    val filtered2 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold2)

    val threshold3 = args(1).toInt
    val filtered3 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold3)

    val threshold4 = args(1).toInt
    val filtered4 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold4)

    val threshold5 = args(1).toInt
    val filtered5 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold5)

    val threshold6 = args(1).toInt
    val filtered6 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold6)

    val threshold7 = args(1).toInt
    val filtered7 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold7)

    val threshold8 = args(1).toInt
    val filtered8 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold8)

    val threshold9 = args(1).toInt
    val filtered9 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold9)

    val threshold10 = args(1).toInt
    val filtered10 = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).filter(_._2 >=threshold10)
	

    val job1 : JobConcurent = new JobConcurent(filtered1, 1, oPath1)
    val job2 : JobConcurent = new JobConcurent(filtered2, 2, oPath2)
    val job3 : JobConcurent = new JobConcurent(filtered3, 3, oPath3)
    val job4 : JobConcurent = new JobConcurent(filtered4, 4, oPath4)
    val job5 : JobConcurent = new JobConcurent(filtered5, 5, oPath5)
    val job6 : JobConcurent = new JobConcurent(filtered6, 6, oPath6)
    val job7 : JobConcurent = new JobConcurent(filtered7, 7, oPath7)
    val job8 : JobConcurent = new JobConcurent(filtered8, 8, oPath8)
    val job9 : JobConcurent = new JobConcurent(filtered9, 9, oPath9)
    val job10 : JobConcurent = new JobConcurent(filtered10, 10, oPath10)

    job1.start()
    job2.start()
    job3.start()
    job4.start()
    job5.start()
    job6.start()
    job7.start()
    job8.start()
    job9.start()
    job10.start()

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
