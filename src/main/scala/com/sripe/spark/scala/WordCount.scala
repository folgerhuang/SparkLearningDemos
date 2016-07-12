package com.sripe.spark.scala

/**
  * Created by shiyu on 6/22/2016.
  */

import org.apache.spark._

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("spark://learnhadoopnode:7077").setAppName("WordCount").setJars(Seq("G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"))
    val sc = new SparkContext(conf)
    val file = sc.textFile("hdfs://learnhadoopnode:9000/user/shiyu/spark/test")
    //val wordContains = file.flatMap(f => f.split(" ")).map(f => (f, 1)).reduceByKey(_ + _).sortByKey()
    //sort the wordcounts by value
    val wordContains = file.flatMap(f => f.split(" ")).map(f => (f, 1)).reduceByKey(_ + _).sortBy(b => -b._2)
    //val wordContains = file.flatMap(f => f.split(" ")).map(f => (f, 1)).reduceByKey(_ + _).map(i => i.swap).sortByKey(false).map(i => i.swap)
    val wordCollect = wordContains.collect()
    wordCollect.foreach(println(_))
  }
}
