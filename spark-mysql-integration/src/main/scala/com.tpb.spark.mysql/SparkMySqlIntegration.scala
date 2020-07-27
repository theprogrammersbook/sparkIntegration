package com.tpb.spark.mysql

import java.io.File
import java.sql.{Connection, DriverManager}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.Directory
/**
   Testing the spark with mysql integration,here
 we are tesing the insertion of data to MySql server with java connection
 Reading  the data from Spark Session.
 */
object SparkMySqlIntegration {
  // required variables.
  val username = "root"
  val password = "root"
  val db = "spark_db"
  val table = "test_json"
  val field = "json"
  val url = "jdbc:mysql://localhost:3306"
  var localOutputFile = "prepared_mysql_data"
  var sc: SparkContext = null
  var connection: Connection = null
  var startTime: DateTime = null
  var endTime: DateTime = null
  val numOfRecordsToCreate = 10

  def main(args: Array[String]): Unit = {
   // Setting log level
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    setupSpark
    createDB

    setupConnection

    createTable
    insertData
    cleanUp

    readData

    printTimeTake
  }

  def setupSpark(): Unit = {
    // Creating spark config and after that creating spark context.
    var conf = new SparkConf()
    conf.setAppName("Sexy Boom Thang")
      .setMaster("local")
        .setAppName("Spark Mysql Integration")
     // .set("spark.hadoop.validateOutputSpecs", "false")
      //Got Exception like : RpcTimeoutException: Futures timed out after [10000 milliseconds]. This timeout is controlled by spark.executor.heartbeatInterval
      //	at org.apache.spark.rpc.RpcTimeout.org$apache$spark$rpc$RpcTimeout$$createRpcTimeoutException(RpcTimeout.scala:47)
      .set("spark.executor.heartbeatInterval","200000")
      .set("spark.network.timeout","300000")

    sc = new SparkContext(conf)
  }

  def createDB(): Unit = {
    // create database if do not exists.
    val connection = DriverManager.getConnection(url, username, password)
    connection.createStatement.executeUpdate(s"CREATE DATABASE IF NOT EXISTS ${db}")
    connection.close
  }

  def setupConnection(): Unit = {
     // As we have already DB , So that Now we are connecting that DB .
    connection = DriverManager.getConnection(s"${url}/${db}", username, password)
  }

  def createTable(): Unit = {
    // Creating a table with one field.
    connection.createStatement.executeUpdate(
      s""" CREATE TABLE IF NOT EXISTS ${table} (
         |   id INTEGER NOT NULL AUTO_INCREMENT,
         |   ${field} TEXT,
         |   PRIMARY KEY ( id )
         |   );
         |   """.stripMargin
    )
  }

  def createRandomJson(limit: Int): List[String] = {
    // Creating list of dummy data
    val exampleJson = """{"name": "nagaraju"}"""
    List.fill(limit)(exampleJson)
  }

  def insertData(): Unit = {
    val data = createRandomJson(numOfRecordsToCreate)

    // start timing...
    startTime = DateTime.now
    // Creating a file and saving that file to file system.
    val collection = sc.parallelize(data)
    collection.saveAsTextFile(localOutputFile)
     // Pushing the data to table , which we have created .
    connection.createStatement.executeUpdate(
      s"""LOAD DATA LOCAL INFILE '${localOutputFile}/part-00000' INTO TABLE ${table} (${field})"""
    )

    // stop timing...
    endTime = DateTime.now
  }

  def readData(): Unit = {
    // creating the spark session object
    val sparkSession = SparkSession.builder().master("local").appName("SparkMySQLApp").getOrCreate()
    // setting the read option , and required options to read data from db table.
    val df = sparkSession.read.format("jdbc")
      .option("url", s"${url}/${db}")
      .option("user", username)
      .option("password", password)
      .option("dbtable", table)

    var readResults = new ArrayBuffer[String]()
     // Different sizes are reading and saving these time taken details in Array.
    for (limit <- List[Int](100, 1000, 100000, 1000000)) {
      val readStartTime = DateTime.now

      val result = df.load.limit(limit).collect()

      val readEndTime = DateTime.now

      readResults += s"Time Taken : ${(readEndTime.getMillis - readStartTime.getMillis) / 1000} seconds to read ${result.size} records"
    }

    readResults.foreach(println)

    sparkSession.stop
  }

  def cleanUp(): Unit = {
   // stop or closing the connection.
    sc.stop
    connection.close
    //deleting the dummy folder which we created while saving the data with the help of RDD.
    val directory = new Directory(new File("./"+localOutputFile))
    directory.deleteRecursively()
  }

  def printTimeTake(): Unit = {
    // Complete operation time taken
    println("*" * 100)
    println(s"Time taken : ${(endTime.getMillis - startTime.getMillis) / 1000} seconds to insert ${numOfRecordsToCreate} records")
    println("*" * 100)
  }
}

