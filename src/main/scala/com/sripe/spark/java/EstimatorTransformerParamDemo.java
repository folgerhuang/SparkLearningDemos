package com.sripe.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

/**
 * Created by shiyu on 7/4/2016.
 */
public class EstimatorTransformerParamDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("spark://learnhadoopnode:7077")
                .setAppName("ML Demo1")
                .setJars(new String[]{"G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"});
        SparkContext sparkContext = new SparkContext(conf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        DataFrame training = sqlContext.createDataFrame(Arrays.asList(
                new LabeledPoint(1.0, Vectors.dense(2, 1.3, -1)),
                new LabeledPoint(1.0, Vectors.dense(2, 1.1, 0.4)),
                new LabeledPoint(0.0, Vectors.dense(0, 3, -1)),
                new LabeledPoint(0.0, Vectors.dense(0, -1, 0.8))
        ), LabeledPoint.class);


    }
}
