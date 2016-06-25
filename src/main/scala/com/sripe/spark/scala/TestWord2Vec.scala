package com.sripe.spark.scala

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shiyu on 6/25/2016.
  */
object TestWord2Vec {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test Word2Vec").setMaster("spark://learnhadoopnode:7077").setJars(Seq("G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"))
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val documentDF = sqlContext.createDataFrame(Seq(
      "苹果 官网 苹果 宣布".split(" "),
      "苹果 梨 香蕉".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(1)
    val mode1 = word2Vec.fit(documentDF)

    val result = mode1.transform(documentDF)
    result.collect().foreach(println)

  }

}
