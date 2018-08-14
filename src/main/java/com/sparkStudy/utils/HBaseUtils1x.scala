package com.sparkStudy.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.slf4j.LoggerFactory

/**
  * @Author: JZ.Lee
  * @Description:HBase1x增删改查
  * @Date: Created at 上午11:02 18-8-14
  * @Modified By:
  */
object HBaseUtils1x {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  private var connection:Connection = null
  private var conf:Configuration = null

  def init() = {
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "lee")
    connection = ConnectionFactory.createConnection(conf)
  }

  def closeConnection(): Unit = {
    connection.close()
  }

  def createTable(tableName: String, family: Array[String]) {
    val admin = connection.getAdmin
    val name: TableName = TableName.valueOf(tableName);
    val desc: HTableDescriptor = new HTableDescriptor(name);
    for (f <- family) {
      desc.addFamily(new HColumnDescriptor(f));
    }
    if (!admin.tableExists(name)) {
      admin.createTable(desc);
    }
  }

  def createTable(tableName: String, family: Array[String],liveTime:Int) {
    val admin = connection.getAdmin
    val name: TableName = TableName.valueOf(tableName);
    val desc: HTableDescriptor = new HTableDescriptor(name);
    for (f <- family) {
      val hColumnDescriptor=new HColumnDescriptor(f)
      hColumnDescriptor.setTimeToLive(liveTime)
      desc.addFamily(hColumnDescriptor)
    }
    if (!admin.tableExists(name)) {
      admin.createTable(desc);
    }
  }

  def getGetAction(rowKey: String):Get = {
    val getAction = new Get(Bytes.toBytes(rowKey));
    getAction.setCacheBlocks(false);
    getAction
  }

  def getPutAction(rowKey: String, familyName:String, column: Array[String], value: Array[String]):Put = {
    val put: Put = new Put(Bytes.toBytes(rowKey));
    for (i <- 0 until(column.length)) {
      put.add(Bytes.toBytes(familyName), Bytes.toBytes(column(i)), Bytes.toBytes(value(i)));
    }
    put;
  }

  def addDataBatchEx(tableName:String, puts:java.util.List[Put]): Unit = {
    val name = TableName.valueOf(tableName)
    val table = connection.getTable(name)
    val listener = new ExceptionListener {
      override def onException
      (e: RetriesExhaustedWithDetailsException, bufferedMutator: BufferedMutator): Unit = {
        for(i <-0 until e.getNumExceptions){
          LOGGER.info("写入put失败:" + e.getRow(i))
        }
      }
    }
    val params = new BufferedMutatorParams(name)
      .listener(listener)
      .writeBufferSize(4*1024*1024)
    try{
      val mutator = connection.getBufferedMutator(params)
      mutator.mutate(puts)
      mutator.close()
    }catch {
      case e:Throwable => e.printStackTrace()
    }
  }

  def scan4All(tableName:String, scan:Scan) = {
    val name = TableName.valueOf(tableName)
    scan.setCacheBlocks(false);
    connection.getTable(name).getScanner(scan);
  }

  def scan4All(tableName:String, startRowKey:String, endRowKey:String, family:String, column:String) = {
    val name = TableName.valueOf(tableName)
    val scan = new Scan();
    scan.setStartRow(Bytes.toBytes(startRowKey));
    scan.setStopRow(Bytes.toBytes(endRowKey));
    scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
    scan.setCacheBlocks(false);
    connection.getTable(name).getScanner(scan);
  }

  def getResults(tableName: String, gets:java.util.List[Get]) : Array[Result] = {
    val name = TableName.valueOf(tableName)
    val table = connection.getTable(name);
    table.get(gets);
  }

  def getResult(tableName: String, rowKey: String): Result = {
    val get: Get = new Get(Bytes.toBytes(rowKey));
    val name = TableName.valueOf(tableName)
    val table = connection.getTable(name);
    table.get(get);
  }

  def deleteTable(tableName: String) {
    val admin = connection.getAdmin
    val name = TableName.valueOf(tableName)
    val table = connection.getTable(name);
    if (admin.tableExists(name)) {
      admin.disableTable(name);
      admin.deleteTable(name);
    }
  }

  def deleteColumn(tableName: String , rowkeys: Set[String]): Unit = {
    val admin = connection.getAdmin
    val name = TableName.valueOf(tableName)
    val table = connection.getTable(name);
    val deletes = new java.util.ArrayList[Delete]()
    rowkeys.foreach(f=>{
      val delete = new Delete(Bytes.toBytes(f))
      deletes.add(delete)
    })
    table.delete(deletes)
  }

}
