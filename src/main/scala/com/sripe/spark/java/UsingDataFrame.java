package com.sripe.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.List;

/**
 * Created by shiyu on 7/11/2016.
 * [shiyu@learnhadoopnode ~]$ cat people.txt
 shi 18
 yu 23
 xu 12
 zhang 21
 */
public class UsingDataFrame {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("spark://learnhadoopnode:7077")
                .setAppName("Using DataFrame")
                .setJars(new String[]{"G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> stringJavaRDD = sc.textFile("hdfs://learnhadoopnode:9000/user/shiyu/spark/people.txt");
        JavaRDD<Person> personJavaRDD = stringJavaRDD.map(new Function<String, Person>() {
            public Person call(String v1) throws Exception {
                String[] split = v1.split(" ");
                Person p = new Person();
                p.setName(split[0]);
                p.setAge(Integer.parseInt(split[1]));
                return p;
            }
        });

        SQLContext sqlContext = new SQLContext(sc);

        DataFrame schemaPeople = sqlContext.createDataFrame(personJavaRDD, Person.class);
        schemaPeople.registerTempTable("people");

        DataFrame teenagers = sqlContext.sql("select name from people where age >=1 and age <= 19");
        List<String> teeagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
            public String call(Row v1) throws Exception {
                return "Name:" + v1.getString(0);
            }
        }).collect();

        for (String str :
                teeagerNames) {
            System.out.println(str);
        }

    }


    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
