package com.sripe.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 * Created by shiyu on 6/24/2016.
 */
public class WordCount {
    public static void main(String[] args) {
        long startTime, endTime;
        startTime=Calendar.getInstance().getTimeInMillis();
        SparkConf sparkConf = new SparkConf().setAppName("WordCount With JAVA")
                .setMaster("spark://learnhadoopnode:7077")
                .setJars(new String[]{"G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"});
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<String> distFile = jsc.textFile("hdfs://learnhadoopnode:9000/user/shiyu/spark/test");
        JavaRDD<String> words = distFile.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        }).persist(StorageLevel.MEMORY_ONLY());

        /*JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).sortByKey();*/
        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }).sortByKey();
        List<Tuple2<String, Integer>> sample = counts.collect();
        for (Tuple2<?, ?> item :
                sample) {
            System.out.println(item.toString());
        }
        endTime=Calendar.getInstance().getTimeInMillis();
        System.out.println("Time elapsed:"+(endTime-startTime)/1000);
    }
}
