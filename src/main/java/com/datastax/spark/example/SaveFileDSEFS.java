package com.datastax.spark.example;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;

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
public final class SaveFileDSEFS {
	private static final Pattern SPACER = Pattern.compile("\n");

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		// Create the context with a 1 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("SaveFileDSEFS");
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);

		//Create output stream to HDFS file
		Path dst = new Path("dsefs://127.0.0.1:5598/tmp/NetCat.java");

		//Create input stream from local file
		Path src = new Path("file:///tmp/NetCat.java");

		fs.copyFromLocalFile(src, dst);
	}
}