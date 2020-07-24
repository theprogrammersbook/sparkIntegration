This project is for integration of spark and hbase
Contains the following..

Used Hadoop, Spark and Hbase Versions are

Hadoop Ver: 2.10.0
Hbase Ver: 2.2.3
Scala Ver: 2.11.12
Spark Ver: 2.4.5

1. I have tested the Hbase Admin API and Client API
2. Spark RDD HBase Integration
3. Spark Streaming Hbase Integration

**Running Problem in Intellij**

Failure to find org.glassfish:javax.el:pom:3.0.1-b06-SNAPSHOT in http://scala-tools.org/repo-releases was cached in the local repository, resolution will not be reattempted until the update interval of scala-tools.org has elapsed or updates are forced
Fix: Run the Application in Maven Command Line: 

  `> mvn clean install -U`
