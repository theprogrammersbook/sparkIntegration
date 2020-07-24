package com.theprogrammersbook.hbase.client

import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

object PutData extends App {
  //Creating HBaseConfiguration
  val configuration = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(configuration)
  //Get table from Connection
  val table = connection.getTable(TableName.valueOf("emp"))
  // Creating row
  val put = new Put(Bytes.toBytes("1"))
  // Adding data
  put.addColumn(Bytes.toBytes("personal data"), Bytes.toBytes("name"), Bytes.toBytes("nagaraju"))
  put.addColumn(Bytes.toBytes("professional data"), Bytes.toBytes("salary"), Bytes.toBytes(10000))
  // Put the data to Hbase
  table.put(put)
  table.close()
}
