package com.theprogrammersbook.hbase.admin

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory

object CreateTable extends App {
  //Creating HBaseConfiguration
  val configuration = HBaseConfiguration.create()
  // Create Connection
  val connection = ConnectionFactory.createConnection()
  // Get Admin
  val admin = connection.getAdmin
  // Provide Table Name
  val tableDescriptor = new HTableDescriptor(TableName.valueOf("EmployeeDetails"))
  // Add Column Family
  tableDescriptor.addFamily(new HColumnDescriptor("personal details"))
  tableDescriptor.addFamily(new HColumnDescriptor("professional details"))
  // Add the collum to Admin

  admin.createTable(tableDescriptor)
  // Check the success message
  println("EmployeeDetails Table is created ...")
}
