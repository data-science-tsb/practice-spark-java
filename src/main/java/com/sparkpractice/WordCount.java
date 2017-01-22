package com.sparkpractice;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String...args) {
        //acquire the spark context
        SparkConf conf = new SparkConf().setAppName("word-count-java");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //load the input file
        JavaRDD<String> textFile = sc.textFile(args[0]);

        //perform count
        JavaPairRDD<String, Integer> counts = textFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        //output the counts
        counts.saveAsTextFile(args[1]);

        //terminate context
        sc.stop();
    }
}
