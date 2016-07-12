package com.sripe.spark.scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shiyu on 7/11/2016.
  */
object UsingDataFrame {
  case class Person(name: String, age: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Using DataFrame")
      .setMaster("spark://learnhadoopnode:7077")
      .setJars(Seq("G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"))
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    import sQLContext.implicits._
    val people = sc.textFile("hdfs://learnhadoopnode:9000/user/shiyu/spark/people.txt").map(_.split(" ")).map(p => Person(p(0), p(1).trim.toInt)).toDF();
    people.registerTempTable("people")

    val teenagers = sQLContext.sql("select * from people where age >= 13 and age <= 19")
    teenagers.map(t => "Name:" + t(0)).collect().foreach(println)
    teenagers.map(t => "Name:" + t.getAs[String]("name")).collect().foreach(println)
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  }
}
