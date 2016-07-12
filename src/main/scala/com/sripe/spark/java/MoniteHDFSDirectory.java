package com.sripe.spark.java;

import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by shiyu on 6/25/2016.
 * monitor hdfs factory.
 */
public class MoniteHDFSDirectory {
    public static void main(String[] args) {
        JavaStreamingContext ssc = new JavaStreamingContext("spark://learnhadoopnode:7077", "Monitor HDFS Directory", Seconds.apply(5), "/opt/spark161_26", new String[]{"G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"});
        JavaDStream<String> stringJavaDStream = ssc.textFileStream("hdfs://learnhadoopnode:9000/user/shiyu/spark/");
       /* JavaDStream<String> numAs = stringJavaDStream.filter(new Function<String, Boolean>() {
            public Boolean call(String v1) throws Exception {
                return v1.contains("a");
            }
        });*/
        stringJavaDStream.print();

        ssc.start();//start the computation
        ssc.awaitTermination();//wait the computation to terminate.

    }
}
