package com.sripe.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by shiyu on 7/4/2016.
 */
public class PiplineDemo {

    public static class Document implements Serializable {
        private long id;
        private String text;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public Document(long id, String text) {
            this.id = id;
            this.text = text;
        }
    }

    public static class LabeledDocument extends Document implements Serializable {
        private double label;


        public LabeledDocument(long id, String text, double label) {
            super(id, text);
            this.label = label;
        }

        public double getLabel() {
            return label;
        }

        public void setLabel(double label) {
            this.label = label;
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("spark://learnhadoopnode:7077")
                .setAppName("ML Demo1")
                .setJars(new String[]{"G:\\AndroidStudioProjects\\SparkLearningDemos\\com.sripe.spark.jar"});
        SparkContext sparkContext = new SparkContext(conf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        // Prepare training documents, which are labeled.
        DataFrame training = sqlContext.createDataFrame(Arrays.asList(
                new LabeledDocument(0L, "a b c d e spark", 1.0),
                new LabeledDocument(1L, "b d", 0.0),
                new LabeledDocument(2L, "x y a", 0.0),
                new LabeledDocument(3L, "spark f g h", 1.0),
                new LabeledDocument(4L, "a ia mi spark h", 1.0),
                new LabeledDocument(5L, "this spark", 1.0),
                new LabeledDocument(6L, "hadoop mapreduce", 0.0)
        ), LabeledDocument.class);
        // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");
        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(100)
                .setRegParam(0.01);
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, hashingTF, lr});

// Fit the pipeline to training documents.
        PipelineModel model = pipeline.fit(training);

// Prepare test documents, which are unlabeled.
        DataFrame test = sqlContext.createDataFrame(Arrays.asList(
                new Document(7L, "spark i j k"),
                new Document(8L, "l m n"),
                new Document(9L, "mapreduce spark"),
                new Document(10L, "apache hadoop")
        ), Document.class);

// Make predictions on test documents.
        DataFrame predictions = model.transform(test);
        for (Row r : predictions.select("id", "text", "probability", "prediction").collect()) {
            System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2)
                    + ", prediction=" + r.get(3));
        }
    }
}
