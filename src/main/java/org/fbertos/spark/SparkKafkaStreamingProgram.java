package org.fbertos.spark;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.google.gson.Gson;

import scala.Tuple2;


public class SparkKafkaStreamingProgram {
	public static void main(String args[]) throws InterruptedException, FileNotFoundException, UnsupportedEncodingException {
		// String file_path = "hdfs://localhost:9000/examples/output.log";
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		
		Set<String> topics = new HashSet<String>(Arrays.asList("input-cm02-file"));

        SparkConf conf = new SparkConf().setAppName("spark-streaming-example");
        JavaStreamingContext sc = new JavaStreamingContext(conf, new Duration(1000));
		
        JavaInputDStream<ConsumerRecord<String, String>> messages = 
        		  KafkaUtils.createDirectStream(
        		    sc, 
        		    LocationStrategies.PreferConsistent(), 
        		    ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
        
        JavaPairDStream<String, String> results = messages
        		  .mapToPair( 
        		      record -> {
        		    	  Gson json = new Gson();
        		    	  Message message = json.fromJson(record.value(), Message.class);
        		    	  //System.out.println(message.getPayload());
        		    	  return new Tuple2<>(record.key(), message.getPayload()); }
        		  );

        JavaDStream<String> lines = results
        		  .map(
        		      tuple2 -> tuple2._2()
        		  );
        
        JavaDStream<String> lines2 = lines.filter(
        			item -> { 
        				String[] list = item.split(",");
        				
        				if (list != null && list.length > 4) {
        					return "9XP".equals(list[4])?true:false;
        				}
        				return false;
        			}
        		);
        
        
        JavaPairDStream<String, Integer> results2 = lines2
      		  .mapToPair( 
      		      record -> {
      		    	String[] list = record.split(",");
      		    	
    				if (list != null && list.length > 3) {
    					System.out.println(list[3].toString());
          		    	if ("CCN".equals(list[3]))
            		    	  return new Tuple2<>(list[3], 1); 
            		    	else
            		    		return new Tuple2<>(list[3], 0);
    				}
    				
    				return new Tuple2<>("", 0);      		    	
      		      }
      		  );
        
        
        JavaPairDStream<String, Integer> results3 = results2.reduceByKey(
        		(k, w) -> {
        			//System.out.println(k.toString());
        			return k + w; });

        /*
        JavaPairDStream<String, Integer> results3 = results2.reduceByKeyAndWindow(
        		(k, w) -> {
        			System.out.println(k.toString());
        			return k + w; }
        		, new Duration(5000), new Duration(1000));
         */
        
        results3.foreachRDD(
        	    javaRdd -> {
        	      Map<String, Integer> wordCountMap = javaRdd.collectAsMap();
        	      for (String key : wordCountMap.keySet()) {
        	        System.out.println(key + "===>" + wordCountMap.get(key));
        	      }
        	    }
        	  );        
        
        
        sc.start();
        sc.awaitTermination();
        sc.close();
	}
}