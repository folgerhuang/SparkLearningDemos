package com.sripe.spark.scala

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, Word2Vec}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by shiyu on 6/25/2016.
  */
object TestCountVectorizer {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test Word2Vec").setMaster("spark://learnhadoopnode:7077").setJars(Seq("G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"))
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.createDataFrame(Seq(
      (0, Array("苹果", "官网", "苹果", "宣布")),
      (1, Array("苹果", "梨", "香蕉"))
    )).toDF("id", "words")

    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(5) //设置词语的总个数，词语编号后的数值均小于该值
      .setMinDF(1) //设置包含词语的最少的文档数
      .fit(df)

    println("output1:")
    cvModel.transform(df).select("id", "words", "features").collect().foreach(println)

    val cvModel2: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3) //设置词语的总个数，词语编号后的数值均小于该值
      .setMinDF(2) //设置包含词语的最少的文档数
      .fit(df)

    println("output2:")
    cvModel2.transform(df).select("id", "words", "features").collect().foreach(println)

  }
}
