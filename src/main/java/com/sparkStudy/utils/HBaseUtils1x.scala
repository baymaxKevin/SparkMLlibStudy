package com.sparkStudy.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
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

  def getJobConf(tableName:String) = {
    val conf = HBaseConfiguration.create()
    val jobConf = new JobConf(conf)
    jobConf.set("hbase.zookeeper.quorum", "lee")
    jobConf.set("hbase.zookeeper.property.clientPort", "2181")
    jobConf.set(org.apache.hadoop.hbase.mapred.TableOutputFormat.OUTPUT_TABLE,tableName)
    jobConf.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])
    jobConf
  }

  def getNewConf(tableName:String) = {
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "lee")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE,tableName)
    val scan = new Scan()
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN,Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
    conf
  }

  def getNewJobConf(tableName:String, sc:SparkContext) = {
    conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","lee")
    conf.set("hbase.zookeeper.property.clientPort","2181")
    conf.set("hbase.defaults.for.version.skip","true")
    conf.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.setClass("mapreduce.job.outputformat.class", classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[String]],classOf[org.apache.hadoop.mapreduce.OutputFormat[String, Mutation]])
    new JobConf(conf)
  }

  def closeConnection(): Unit = {
    connection.close()
  }

  def createTable(tableName: String, family: Array[String]) {
    val admin = connection.getAdmin
    val name: TableName = TableName.valueOf(tableName)
    val desc: HTableDescriptor = new HTableDescriptor(name)
    for (f <- family) {
      desc.addFamily(new HColumnDescriptor(f))
    }
    if (!admin.tableExists(name)) {
      admin.createTable(desc)
    }
  }

  def createTable(tableName: String, family: Array[String],liveTime:Int) {
    val admin = connection.getAdmin
    val name: TableName = TableName.valueOf(tableName)
    val desc: HTableDescriptor = new HTableDescriptor(name)
    for (f <- family) {
      val hColumnDescriptor=new HColumnDescriptor(f)
      // 设置blockcache大小
      hColumnDescriptor.setBlocksize(8192)
      // 设置列簇的生命周期
      hColumnDescriptor.setTimeToLive(liveTime)
      desc.addFamily(hColumnDescriptor)
    }
    if (!admin.tableExists(name)) {
      admin.createTable(desc)
    }
  }

  def getGetAction(rowKey: String):Get = {
    val getAction = new Get(Bytes.toBytes(rowKey))
    // 查询默认缓存，设置为false可以节省交换缓存操作消耗
    getAction.setCacheBlocks(false)
    getAction
  }

  def getPutAction(rowKey: String, familyName:String, column: Array[String], value: Array[String]):Put = {
    val put: Put = new Put(Bytes.toBytes(rowKey))
    for (i <- 0 until(column.length)) {
      put.add(Bytes.toBytes(familyName), Bytes.toBytes(column(i)), Bytes.toBytes(value(i)))
      // 关闭WAL机制
      put.setDurability(Durability.SKIP_WAL)
    }
    put
  }

  def insertData(tableName:String, put: Put) = {
    val name = TableName.valueOf(tableName)
    val table = connection.getTable(name)
    table.put(put)
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
    // 设置客户端写buffer大小，达到阈值则flush
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
    scan.setCacheBlocks(false)
    connection.getTable(name).getScanner(scan)
  }

  def scan4All(tableName:String, startRowKey:String, endRowKey:String, family:String, column:String) = {
    val name = TableName.valueOf(tableName)
    val scan = new Scan()
    scan.withStartRow(Bytes.toBytes(startRowKey))
    scan.withStopRow(Bytes.toBytes(endRowKey))
    scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column))
    scan.setCacheBlocks(false)
    connection.getTable(name).getScanner(scan)
  }

  def getResults(tableName: String, gets:java.util.List[Get]) : Array[Result] = {
    val name = TableName.valueOf(tableName)
    val table = connection.getTable(name)
    table.get(gets)
  }

  def getResult(tableName: String, rowKey: String): Result = {
    val get: Get = new Get(Bytes.toBytes(rowKey))
    val name = TableName.valueOf(tableName)
    val table = connection.getTable(name)
    table.get(get)
  }

  def deleteTable(tableName: String) {
    val admin = connection.getAdmin
    val name = TableName.valueOf(tableName)
    val table = connection.getTable(name)
    if (admin.tableExists(name)) {
      admin.disableTable(name)
      admin.deleteTable(name)
    }
  }

  def deleteColumn(tableName: String , rowkeys: Set[String]): Unit = {
    val admin = connection.getAdmin
    val name = TableName.valueOf(tableName)
    val table = connection.getTable(name)
    val deletes = new java.util.ArrayList[Delete]()
    rowkeys.foreach(f=>{
      val delete = new Delete(Bytes.toBytes(f))
      deletes.add(delete)
    })
    table.delete(deletes)
  }

}
