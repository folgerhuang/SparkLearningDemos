package com.sripe.spark.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by shiyu on 6/25/2016.
  * nc -lp 9999
  * netstat -an|grep 9999 （查看端口是否正常打开）
  */
object NewWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("New Word Count")
      .setMaster("spark://learnhadoopnode:7077")
      .setJars(Seq("G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"))
    val ssc = new StreamingContext(conf,Seconds(2))
    val lines = ssc.socketTextStream("learnhadoopnode",9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCount = pairs.reduceByKey((a,b) => a+b)
    wordCount.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
