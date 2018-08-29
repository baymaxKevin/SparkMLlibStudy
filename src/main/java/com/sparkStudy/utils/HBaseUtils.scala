//package com.sparkStudy.utils
//
//import java.util
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener
//import org.apache.hadoop.hbase.client._
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
//import org.slf4j.LoggerFactory
//
///**
//  * @Author: JZ.Lee
//  * @Description: HBase增删改查询
//  * @Date: Created at 下午6:13 18-8-10
//  * @Modified By:
//  */
//object HBaseUtils {
//
//  private val LOGGER = LoggerFactory.getLogger(this.getClass)
//  private var connection : Connection = null
//  private var conf: Configuration = null
//
//  private val comp = new util.HashMap[String, CompareOperator]()
//  comp.put("EQUAL",CompareOperator.EQUAL)
//  comp.put("GREATER",CompareOperator.GREATER)
//  comp.put("GREATER_OR_EQUAL",CompareOperator.GREATER_OR_EQUAL)
//  comp.put("LESS",CompareOperator.LESS)
//  comp.put("LESS_OR_EQUAL",CompareOperator.LESS_OR_EQUAL)
//  comp.put("NO_OP",CompareOperator.NO_OP)
//  comp.put("NOT_EQUAL",CompareOperator.NOT_EQUAL)
//
//  def init() = {
//    /**
//     * @Author:JZ.Lee
//     * @Description:hbase连接初始化
//     * @Date: 下午2:38 18-8-12
//     * @Param []
//     * @return void
//     */
//    conf = HBaseConfiguration.create()
//    conf.set("hbase.zookeeper.quorum","lee")
//    connection = ConnectionFactory.createConnection(conf)
//  }
//
//  /**
//    * @Author:JZ.Lee
//    * @Description:hbase连接断开
//    * @Date: 下午2:38 18-8-12
//    * @Param []
//    * @return void
//    */
//  def closeConnection() = {
//    connection.close()
//  }
//
//  /**
//    * @Author:JZ.Lee
//    * @Description:创建hbase表，考虑预分区和生命周期、blockcache等建表
//    * @Date: 下午2:39 18-8-12
//    * @Param [tableName, familyNames]
//    * @return boolean
//    */
//  def createTable(tableName:String,
//                  familyNames:Array[String])
//  = {
//    val admin = connection.getAdmin
//    val name = TableName.valueOf(tableName)
////    创建表descriptor，HBase2.0.0之后HTable替换为TableDescriptor
//    val tableDesc = TableDescriptorBuilder.newBuilder(name)
//    if(admin.tableExists(name)) false
//    try{
////      添加列族变更
//      familyNames.foreach(
//        familyName=>{
//          tableDesc.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes
//            .toBytes(familyName))
//            .build())
//        }
//      )
////      创建表
//      admin.createTable(tableDesc.build())
//      true
//    }catch {
//      case e:Throwable => e.printStackTrace()
//        false
//    }
//  }
//
//  /**
//    * @Author:JZ.Lee
//    * @Description:
//    * @Date: 下午2:44 18-8-12
//    * @Param [tableName, familyNames, liveTime]
//    * @return boolean
//    */
//  def createTable(tableName:String,
//                  familyNames:Array[String],liveTime:Int)
//  = {
//    val admin = connection.getAdmin
//    val name = TableName.valueOf(tableName)
////    创建表descriptor，HBase2.0.0之后HTable替换为TableDescriptor
//    val tableDesc = TableDescriptorBuilder.newBuilder(name)
//    if(admin.tableExists(name)) false
//    try{
////      添加列族变更，设置生命周期
//      familyNames.foreach(
//        familyName=>{
//          tableDesc.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes
//            .toBytes(familyName)).setTimeToLive(liveTime)
//            .build())
//        }
//      )
////      创建表
//      admin.createTable(tableDesc.build())
//      true
//    }catch {
//      case e:Throwable => e.printStackTrace()
//        false
//    }
//  }
//
//  def getPutAction(rowKey:String, familyName:String,
//                   columns:Array[String],
//                   values:Array[String]):Put = {
//    val put:Put = new Put(Bytes.toBytes(rowKey))
//    for(i <- 0 until(columns.length)){
//      put.addColumn(Bytes.toBytes(familyName),Bytes
//        .toBytes(columns(i)),Bytes.toBytes(values(i)))
//    }
//    put
//  }
//
//  def insertDataBatchEx(tableName:String,puts:java.util.List[Put]) = {
//    /**
//     * @Author:JZ.Lee
//     * @Description:异步批量写入HBase
//     * @Date: 下午3:34 18-8-11
//     * @Param [tableName, puts]
//     * @return void
//     */
//    val name = TableName.valueOf(tableName)
////    监听hbase写入失败
//    val listener = new ExceptionListener {
//      override def onException
//      (e: RetriesExhaustedWithDetailsException, bufferedMutator: BufferedMutator): Unit = {
//        for(i <- e.getNumExceptions){
//          LOGGER.info("写入put失败: " + e.getRow(i) )
//        }
//      }
//    }
////    BufferedMutatorParams替代HTable.setAutoFlush
//    val params = new BufferedMutatorParams(name).listener(listener)
////    设置写入缓冲区大小
//    params.writeBufferSize(4*1024*1024)
//    try{
//      val mutator = connection.getBufferedMutator(params)
//      mutator.mutate(puts)
//      mutator.close()
//    }
//  }
//
//  /**
//    * @Author:JZ.Lee
//    * @Description:全表扫描
//    * @Date: 下午2:45 18-8-12
//    * @Param [tableName]
//    * @return org.apache.hadoop.hbase.client.ResultScanner
//    */
//  def getScan(tableName:String) = {
//    val table = connection.getTable(TableName.valueOf(tableName))
//    val scaner = new Scan()
//    scaner.setCacheBlocks(false)
//    table.getScanner(scaner)
//  }
//
//  /**
//    * @Author:JZ.Lee
//    * @Description:扫描指定范围rowkey
//    * @Date: 下午2:46 18-8-12
//    * @Param [tableName, startRowKey, endRowKey, family, column]
//    * @return org.apache.hadoop.hbase.client.ResultScanner
//    */
//  def getScan(tableName:String, startRowKey:String,
//              endRowKey:String, family:String,
//              column:String) = {
//    val table = connection.getTable(TableName.valueOf(tableName))
//    val scaner = new Scan()
//      .withStartRow(Bytes.toBytes(startRowKey))
//      .withStopRow(Bytes.toBytes(endRowKey))
//
//    scaner.addColumn(Bytes.toBytes(family),Bytes.toBytes(column))
//    scaner.setCacheBlocks(false)
//    table.getScanner(scaner)
//  }
//
//  def getResult(tableName:String,rowKey:String) = {
//    val get = new Get(Bytes.toBytes(rowKey))
//    val table = connection.getTable(TableName.valueOf(tableName))
//    table.get(get)
//  }
//
//  def getResults(tableName:String,gets:java.util
//  .List[Get]) = {
//    connection.getTable(TableName.valueOf(tableName)).get(gets)
//  }
//
//  /**
//    * @Author:JZ.Lee
//    * @Description:删除表
//    * @Date: 下午2:48 18-8-12
//    * @Param [tableName]
//    * @return boolean
//    */
//  def deleteTable(tableName:String) = {
//    val admin = connection.getAdmin
//    val name = TableName.valueOf(tableName)
//    try{
//      if(admin.tableExists(name)){
//        admin.disableTable(name)
//        admin.deleteTable(name)
//      }
//    }catch {
//      case e:Throwable=>e.printStackTrace()
//        false
//    }
//    true
//  }
//
//  /**
//    * @Author:JZ.Lee
//    * @Description:删除表指定rowKey行
//    * @Date: 下午2:50 18-8-12
//    * @Param [tableName, rowKey]
//    * @return void
//    */
//  def deleteColumn(tableName:String,rowKey:String) = {
//    val table = connection.getTable(TableName.valueOf(tableName))
//    table.delete(new Delete(Bytes.toBytes(rowKey)))
//  }
//
//  /**
//    * @Author:JZ.Lee
//    * @Description:删除并更新指定value
//    * @Date: 下午2:52 18-8-12
//    * @Param [tableName, rowKey, family, column, compareon, value, newValue]
//    * @return boolean
//    */
//  def updateByDelete(tableName:String,rowKey:String,
//             family:String,column:String,
//             compareon:String,value:String,
//             newValue:String) = {
//    val name = TableName.valueOf(tableName)
//    if(comp.get(compareon.toUpperCase) == null) false
//    val table = connection.getTable(name)
//    //hbase2x delete和put继承Mutation，RowMutations.add建议采用Mutation
//    val delete:Mutation = new Delete(Bytes.toBytes
//    (rowKey)).addColumn(Bytes.toBytes(family),Bytes.toBytes(column))
//    val mut = new RowMutations(Bytes.toBytes(rowKey))
//    mut.add(delete)
//    val put:Mutation = new Put(Bytes.toBytes(rowKey))
//      .addColumn(Bytes.toBytes(family),Bytes.toBytes
//      (column),Bytes.toBytes(newValue))
//    mut.add(put)
//    println("------" + comp.get(compareon.toUpperCase()))
//    //hbase2x checkAndMutate建议采用lambda api
//    table.checkAndMutate(Bytes.toBytes(rowKey),Bytes
//      .toBytes(family)).qualifier(Bytes.toBytes(column))
//      .ifMatches(comp.get(compareon.toUpperCase()),Bytes.toBytes(value))
//      .thenMutate(mut)
//  }
//
//  /**
//    * @Author:JZ.Lee
//    * @Description:更新指定value
//    * @Date: 下午2:53 18-8-12
//    * @Param [tableName, rowKey, family, column, compareon, value, newValue]
//    * @return boolean
//    */
//  def updateByCover(tableName:String,rowKey:String,
//                    family:String,column:String,
//                    compareon:String,value:String,
//                    newValue:String) = {
//    val name = TableName.valueOf(tableName)
//    if(comp.get(compareon.toUpperCase) == null) false
//    val table = connection.getTable(name)
//    val mut = new RowMutations(Bytes.toBytes(rowKey))
//    val put:Mutation = new Put(Bytes.toBytes(rowKey))
//      .addColumn(Bytes.toBytes(family),Bytes.toBytes
//      (column),Bytes.toBytes(newValue))
//    mut.add(put)
//    table.checkAndMutate(Bytes.toBytes(rowKey),Bytes
//      .toBytes(family)).qualifier(Bytes.toBytes(column))
//      .ifMatches(comp.get(compareon.toUpperCase()),Bytes.toBytes(value))
//      .thenMutate(mut)
//  }
//
//  /**
//    * @Author:JZ.Lee
//    * @Description:指定时间戳查询数据
//    * @Date: 下午2:54 18-8-12
//    * @Param [tableName, rowKey, family, column, timestamp]
//    * @return org.apache.hadoop.hbase.client.Result
//    */
//  def findByOldTime(tableName:String,rowKey:String,
//                    family:String,column:String,
//                    timestamp:Long) = {
//    val name = TableName.valueOf(tableName)
//    val table = connection.getTable(name)
//    val get = new Get(Bytes.toBytes(rowKey))
//    get.setTimestamp(timestamp)
//    get.addColumn(Bytes.toBytes(family),Bytes.toBytes(column))
//    table.get(get)
//  }
//}
