This Application is for checking the Integration of Spark with MySql.

### we are using the following software.

1. Spark Ver: 2.4.5
2. Mysql Ver: 5.7.29

#### mysql java connector.

Version: 5.1.40

How to run the Application 
1. In IDE
Run SparkMySqlIntegration.scala
2. As Spark Submit
  - a. Create jar file by running command as mvn clean install (if any jar file is not downloadding then use [-U])
  - b. Use IDE provided options to create jar file.
  - c. As we are using mysql java connector jar file so that we have to add this jar file to spark jars folder.
  - d. Go to the spark bin folder or if you have set the this bin folder in path, then we can run the following commond.
     ` spark-submit --class "com.tpb.spark.mysql.SparkMySqlIntegration" --master local[*] spark-mysql-integration-1.0-SNAPSHOT.jar` 
  
   



