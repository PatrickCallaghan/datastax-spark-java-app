# Spark IoT
====================

## Scenario

## Schema Setup
Note : This will drop the keyspace "datastax" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take multiple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To run locally 

	~/dse-5.1.5/bin/dse spark-submit --class com.datastax.spark.example.JavaSensorData target/datastax-spark-java-app-0.1-SNAPSHOT.jar localhost 9999

Run the NetCat class to produce data on the port 9999. This 