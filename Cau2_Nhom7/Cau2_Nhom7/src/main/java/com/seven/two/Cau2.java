package com.seven.two;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Cau2 {
    public static void main(String[] args) throws IOException, IOException {

        if (args.length < 2) {
            System.err.println("Missing input/output path");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("WordCount");
        sparkConf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC");
        sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> textFile = sparkContext.textFile(inputPath).cache();


        JavaPairRDD<String, Integer> javaPairRDDWordCount = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .filter(word -> word.length() >= 5 && word.length() <= 9)
                .mapToPair(word -> new Tuple2<>(Integer.toString(word.length()), 1))
                .reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> wordCount = javaPairRDDWordCount.collect();

        BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath));
        for (Tuple2<String, Integer> row: wordCount) {
            writer.write("- " + row._1() + ": " + row._2() + "\n");
        }
        writer.close();

        sparkContext.close();
    }
}
