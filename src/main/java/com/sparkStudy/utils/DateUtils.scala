package com.sparkStudy.utils

import java.text.SimpleDateFormat
import java.util.Calendar

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

  def getDay(day:Int):String = {
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal:Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, day)
    var dateStr = dateFormat.format(cal.getTime)
    dateStr
  }
}
