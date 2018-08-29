package com.sparkStudy.utils

import java.util.Date

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-8-28 下午4:28
  * @Modified By:
  */
object SparkRWHBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkRWHBase")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val keyword = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header",false)
      .option("delimiter",",")
      .load("file:/opt/data/keyword_catalog_day.csv")

    val tableName1 = "keyword1"
    val tableName2 = "keyword2"
    val tableName3 = "keyword3"
    val tableName4 = "keyword4"
    val cf = "info"
    val columns = Array("keyword", "app_id", "catalog_name", "keyword_catalog_pv", "keyword_catalog_pv_rate")

    val start_time1 = new Date().getTime
    keyword.foreachPartition(records =>{
      HBaseUtils1x.init()
      records.foreach(f => {
        val keyword = f.getString(0)
        val app_id = f.getString(1)
        val catalog_name = f.getString(2)
        val keyword_catalog_pv = f.getString(3)
        val keyword_catalog_pv_rate = f.getString(4)
        val rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes(keyword+app_id)).substring(0,8)
        val cols = Array(keyword,app_id,catalog_name,keyword_catalog_pv,keyword_catalog_pv_rate)
        HBaseUtils1x.insertData(tableName1, HBaseUtils1x.getPutAction(rowKey, cf, columns, cols))
      })
      HBaseUtils1x.closeConnection()
    })
    var end_time1 =new Date().getTime
    println("HBase逐条插入运行时间为：" + (end_time1 - start_time1))

    val start_time2 = new Date().getTime
    keyword.foreachPartition(records =>{
      HBaseUtils1x.init()
      val puts = ArrayBuffer[Put]()
      records.foreach(f => {
        val keyword = f.getString(0)
        val app_id = f.getString(1)
        val catalog_name = f.getString(2)
        val keyword_catalog_pv = f.getString(3)
        val keyword_catalog_pv_rate = f.getString(4)
        val rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes(keyword+app_id)).substring(0,8)
        val cols = Array(keyword,app_id,catalog_name,keyword_catalog_pv,keyword_catalog_pv_rate)
        try{
          puts.append(HBaseUtils1x.getPutAction(rowKey,
            cf, columns, cols))
        }catch{
          case e:Throwable => println(f)
        }
      })
      import collection.JavaConverters._
      HBaseUtils1x.addDataBatchEx(tableName2, puts.asJava)
      HBaseUtils1x.closeConnection()
    })
    val end_time2 = new Date().getTime
    println("HBase批量插入运行时间为：" + (end_time2 - start_time2))

    val start_time3 = new Date().getTime
    keyword.rdd.map(f =>{
      val keyword = f.getString(0)
      val app_id = f.getString(1)
      val catalog_name = f.getString(2)
      val keyword_catalog_pv = f.getString(3)
      val keyword_catalog_pv_rate = f.getString(4)
      val rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes(keyword+app_id)).substring(0,8)
      val cols = Array(keyword,app_id,catalog_name,keyword_catalog_pv,keyword_catalog_pv_rate)
      (new ImmutableBytesWritable, HBaseUtils1x.getPutAction(rowKey, cf, columns, cols))
    }).saveAsHadoopDataset(HBaseUtils1x.getJobConf(tableName3))
    val end_time3 = new Date().getTime
    println("saveAsHadoopDataset方式写入运行时间为：" + (end_time3 - start_time3))
//
    val start_time4 = new Date().getTime
    keyword.rdd.map(f =>{
      val keyword = f.getString(0)
      val app_id = f.getString(1)
      val catalog_name = f.getString(2)
      val keyword_catalog_pv = f.getString(3)
      val keyword_catalog_pv_rate = f.getString(4)
      val rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes(keyword+app_id)).substring(0,8)
      val cols = Array(keyword,app_id,catalog_name,keyword_catalog_pv,keyword_catalog_pv_rate)
      (new ImmutableBytesWritable, HBaseUtils1x.getPutAction(rowKey, cf, columns, cols))
    }).saveAsNewAPIHadoopDataset(HBaseUtils1x.getNewJobConf(tableName4,spark.sparkContext))
    val end_time4 = new Date().getTime
    println("saveAsNewAPIHadoopDataset方式写入运行时间为：" + (end_time4 - start_time4))

    val hbaseRdd = spark.sparkContext.newAPIHadoopRDD(HBaseUtils1x.getNewConf(tableName1), classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println(hbaseRdd.count())
    hbaseRdd.foreach{
      case(_,result) => {
        //        获取行键
        val rowKey = Bytes.toString(result.getRow)
        val keyword = Bytes.toString(result.getValue(cf.getBytes(), "keyword".getBytes()))
        val keyword_catalog_pv_rate = Bytes.toDouble(result.getValue(cf.getBytes(), "keyword_catalog_pv_rate".getBytes()))
        println(rowKey + "," + keyword + "," + keyword_catalog_pv_rate)
      }
    }
  }
}
