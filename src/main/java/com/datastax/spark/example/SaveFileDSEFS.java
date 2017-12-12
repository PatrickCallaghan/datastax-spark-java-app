package com.datastax.spark.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 */
public final class SaveFileDSEFS {

	public static void main(String[] args) throws Exception {

		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);

		//Create output stream to HDFS file
		Path dst = new Path("dsefs://127.0.0.1:5598/tmp/NetCat.java");

		//Create input stream from local file
		Path src = new Path("file:///tmp/NetCat.java");

		fs.copyFromLocalFile(src, dst);
	}
}