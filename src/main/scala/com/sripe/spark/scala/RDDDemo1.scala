package com.sripe.spark.scala

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shiyu on 6/27/2016.
  */
object RDDDemo1 {
  def main(args: Array[String]) {
    val startTime = Calendar.getInstance().getTimeInMillis();
    val conf = new SparkConf().setMaster("spark://learnhadoopnode:7077").setAppName("RDD Demo1").setJars(Seq("G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"))
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://learnhadoopnode:9000/user/shiyu/input/yarn-site.xml")
    val words = lines.flatMap(_.split("""\s+""")).filter(_.nonEmpty)
    val counts = words.groupBy(_.toLowerCase()).map{case (w,all) => (w,all.size)}
    counts.sortBy(_._2,false).collect().foreach(println(_))
    val endTime = Calendar.getInstance().getTimeInMillis();
    println("The job last(ms):"+(endTime-startTime))
  }
}
