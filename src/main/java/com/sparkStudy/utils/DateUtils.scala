package com.sparkStudy.utils

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-10-29 下午5:20
  * @Modified By:
  */
object DateUtils {
  /**
    * 获取两周前时间
    * @return String
    */
  def get2WeekAgo():String = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var cal:Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -14)
    var dataStr = dateFormat.format(cal.getTime)
    dataStr
  }

  /**
    * 获取当前时间
    * @return String
    */
  def getNow():String = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var cal:Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, 0)
    var dataStr = dateFormat.format(cal.getTime)
    dataStr
  }

  /**
    * 获取前n天时间
    * @param day
    * @return String
    */
  def getDay(day:Int):String = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal:Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, day)
    var dateStr = dateFormat.format(cal.getTime)
    dateStr
  }

  /**
    * 获取小时时间
    * @param time
    * @return
    */
  def getLogHour(time:String):String = {
    try{
      time.split(" ")(1).split(":")(0)
    }catch{
      case e:Throwable => "0"
    }
  }

  /**
    * 获取hdfs下目录文件
    * @param hdfsPath
    */
  def getHdfsPath(hdfsPath:String) = {
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(hdfsPath),conf)
    val fs = hdfs.listStatus(new Path(hdfsPath))
    val listPath = FileUtil.stat2Paths(fs)
    for(l <- listPath){
      println(l.getName)
    }
  }

  def main(args: Array[String]): Unit = {
    val hdfsPath = "hdfs://lee:8020/"
    getHdfsPath(hdfsPath)
  }
}
