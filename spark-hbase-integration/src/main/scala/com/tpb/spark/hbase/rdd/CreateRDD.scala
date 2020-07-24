package com.tpb.spark.hbase.rdd

import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD extends  App{
  val sparkConf = new SparkConf().setAppName("HBaseMapPartitionExample ").setMaster("local[4]")
  val sc = new SparkContext(sparkConf)
  println(sc.appName)
}
