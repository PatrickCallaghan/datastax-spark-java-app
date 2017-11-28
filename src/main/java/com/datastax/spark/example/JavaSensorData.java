package com.datastax.spark.example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network
 * every second.
 *
 * Usage: JavaNetworkWordCount <hostname> <port> <hostname> and <port> describe
 * the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server `$
 * nc -lk 9999` and then run the example `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaNetworkWordCount localhost 9999`
 */
public final class JavaSensorData {
	private static final Pattern SPACER = Pattern.compile("\n");

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaSensorData <hostname> <port>");
			System.exit(1);
		}

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("JavaSensorData");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0], Integer.parseInt(args[1]),
				StorageLevels.MEMORY_AND_DISK_SER);

		
		JavaDStream<String> readings = lines.flatMap(x -> Arrays.asList(SPACER.split(x)).iterator());
		
		JavaDStream<SensorReading> readingsPerSensor = readings.map(reading -> {

			String[] readingArray = reading.split(";");
			SensorReading record = new SensorReading(readingArray[0], Integer.parseInt(readingArray[1]),
					new java.util.Date(Long.parseLong(readingArray[2])));
			return record;
		});
		
		readingsPerSensor.foreachRDD(new VoidFunction<JavaRDD<SensorReading>>() {
		      
			public void call(JavaRDD<SensorReading> sensorReadingRDD) throws Exception {
		        
		        CassandraJavaUtil.javaFunctions(sensorReadingRDD)
		                .writerBuilder("datastax", "device_data", CassandraJavaUtil.mapToRow(SensorReading.class))
		                .saveToCassandra();
			}
		});
		
		JavaPairDStream<String, SensorReading> pairs = readingsPerSensor.mapToPair(s -> new Tuple2<>(s.getId(), s));		
		JavaPairDStream<String, SensorReading> windowed = pairs.reduceByKeyAndWindow((sr1, sr2) -> new SensorReading(sr1.getId(), sr1.getValue() + sr2.getValue(), sr1.getTime()), 
				Durations.seconds(30), Durations.seconds(10));
		
		windowed.foreachRDD(new VoidFunction<JavaPairRDD<String, SensorReading>>() {
		    @Override
		    public void call(JavaPairRDD<String, SensorReading> arg0) throws Exception {
		    	Map<String, SensorReading> map = arg0.collectAsMap();		    	
		    	Iterator<SensorReading> iterator = map.values().iterator();
		    	while (iterator.hasNext()){
		    		SensorReading sr = iterator.next();
		    		if (sr.getValue() > 650){
		    			System.out.println("Trigger alert for " + sr.getId() + " " + sr.getValue());
		    		}
		    	}
		    }
		});
		
		
		ssc.start();
		ssc.awaitTermination();
	}
}