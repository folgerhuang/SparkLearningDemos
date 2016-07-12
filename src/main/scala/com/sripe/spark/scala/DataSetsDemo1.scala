package com.sripe.spark.scala

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by shiyu on 6/27/2016.
  */
object DataSetsDemo1 {
  def main(args: Array[String]) {
    val startTime = Calendar.getInstance().getTimeInMillis();
    val conf = new SparkConf().setMaster("spark://learnhadoopnode:7077").setAppName("RDD Demo1").setJars(Seq("G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val lines = sqlContext.read.text("hdfs://learnhadoopnode:9000/user/shiyu/input/yarn-site.xml").as[String]
    val words = lines.flatMap(_.split("""\s+""")).filter(_.nonEmpty)
    val count = words.groupBy(_.toLowerCase()).count()
    count.collect().foreach(println(_))
    val endTime = Calendar.getInstance().getTimeInMillis();
    println("The job last(ms):"+(endTime-startTime))
  }
}
