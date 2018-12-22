package org.fbertos.spark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreamingProgram {
	public static void main(String args[]) throws InterruptedException, FileNotFoundException, UnsupportedEncodingException {
        SparkConf conf = new SparkConf().setAppName("spark-streaming-example");
        JavaStreamingContext sc = new JavaStreamingContext(conf, new Duration(1000));
        
        //JavaReceiverInputDStream<String> lines = sc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY());
        JavaDStream<String> lines = sc.textFileStream("hdfs://localhost:9000/examples/input/");
        
        JavaDStream<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        
        JavaPairDStream<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
        		.reduceByKey((a, b) -> a + b);
                

        counts.print();
        
        sc.start();
        sc.awaitTermination();
        sc.close();
    }
}