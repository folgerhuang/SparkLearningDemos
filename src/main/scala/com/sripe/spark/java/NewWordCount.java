package com.sripe.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

/**
 * Created by shiyu on 6/25/2016.
 */
public class NewWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Word count with spark streaming").setMaster("spark://learnhadoopnode:7077")
                .setJars(new String[]{"G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"});
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Seconds.apply(2));
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("learnhadoopnode", 9999);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairDStream<String, Integer> wordPaire = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> counts = wordPaire.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        counts.print();
        jsc.start();//start the computation
        jsc.awaitTermination();//wait for computation to terminate.
    }
}
