package com.hbase.producer

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.LogManager

case class User(username: String, transaction_limit: Int)

object Main {

  @transient lazy val log = LogManager.getLogger(getClass)

  val rnd = new scala.util.Random

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      log.error("Usage: HBaseProducer [quorum]")
      log.error("Example: HBaseProducer localhost")
      System.exit(1)
    }

    log.info("Connecting to hbase...")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", args(0))
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin()
    log.info("OK")

    val tableName = TableName.valueOf("users")

    if (admin.tableExists(tableName)) {
      log.info("Table exists, deleting...")
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
      log.info("OK")
    }

    log.info("Creating table...")
    val tableDesc = new HTableDescriptor(tableName)
    tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("data")))
    admin.createTable(tableDesc)
    log.info("OK")

    log.info("Putting data...")
    val table = conn.getTable(tableName)
    for (i <- 1 to 50) {
      val rowKey = "user" + i
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("transaction_limit"), Bytes.toBytes(500))
      table.put(put)
    }
    log.info("OK")

    log.info("Closing hbase producer...")
    table.close()
    conn.close()
    log.info("OK")
  }
}
