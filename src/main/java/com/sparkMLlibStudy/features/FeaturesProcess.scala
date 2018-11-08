package com.sparkMLlibStudy.features

import com.sparkStudy.utils.DateUtils
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}
import org.apache.spark.sql.{DataFrame, RowFactory, SparkSession, types}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.parsing.json.JSONObject

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-10-30 上午9:33
  * @Modified By:
  */
object FeaturesProcess {
  def main(args: Array[String]): Unit = {
    //设置服务日志级别为ERROR
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("FeaturesProcess")
      .getOrCreate()

    spark.conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.kryoserializer.buffer.max","512m")

    val path = "/opt/data/feeds/logs/part-00001"
    val data = log2Features(spark,path).repartition(4)
    val test_x = data.filter(_.itemId == "28890237")
    println("************" + test_x.count() + "," + test_x.map(_.itemId).take(1).toString)

    // itemId去重升序，并编号
    var itemIdIndexMapTemp = data.map(_.itemId).filter(_!="").distinct().map(_.toInt)
      .sortBy(f=>f,true,1)
      .zipWithIndex().map(f=>(f._1,f._2.toInt))
      .collectAsMap().map(f=>f._1.toString -> f._2)
    // 索引不到itemId给默认-1并编号
    if(!itemIdIndexMapTemp.contains("-1")) itemIdIndexMapTemp += ("-1" -> itemIdIndexMapTemp.size)
    val itemIdIndexMap = itemIdIndexMapTemp.map(f=>f._1.toInt -> f._2)

    // itemKeyword去重升序，并编号
    var itemKeywordIndexMapTemp = data.map(_.itemKtW).filter(_!="").distinct().map(_.toInt)
      .sortBy(f=>f,true,1)
      .zipWithIndex().map(f=>(f._1,f._2.toInt))
      .collectAsMap().map(f=>f._1.toString -> f._2)
    // 索引不到itemKeyword给默认-1并编号
    if(!itemKeywordIndexMapTemp.contains("-1")) itemKeywordIndexMapTemp += ("-1" -> itemKeywordIndexMapTemp.size)
    val itemKeywordIndexMap = itemKeywordIndexMapTemp.map(f=>f._1.toInt -> f._2)

    // itemTag1去重升序，并并编号
    var itemTag1IndexMapTemp = data.map(_.itemTag1).filter(_!="").distinct().map(_.toInt)
      .sortBy(f=>f,true,1)
      .zipWithIndex().map(f=>(f._1,f._2.toInt))
      .collectAsMap().map(f=>f._1.toString -> f._2)
    // 索引不到itemTag1给默认-1并编号
    if(!itemTag1IndexMapTemp.contains("-1")) itemTag1IndexMapTemp += ("-1" -> itemTag1IndexMapTemp.size)
    val itemTag1IndexMap = itemTag1IndexMapTemp.map(f=>f._1.toInt -> f._2)

    // itemTag2去重升序，并编号
    var itemTag2IndexMapTemp = data.map(_.itemTag2).filter(_!="").distinct().map(_.toInt)
      .sortBy(f=>f,true,1)
      .zipWithIndex().map(f=>(f._1,f._2.toInt))
      .collectAsMap().map(f=>f._1.toString -> f._2)
    // 索引不到itemTag2给默认-1并编号
    if(!itemTag2IndexMapTemp.contains("-1")) itemTag2IndexMapTemp += ("-1" -> itemTag2IndexMapTemp.size)
    val itemTag2IndexMap = itemTag2IndexMapTemp.map(f=>f._1.toInt -> f._2)

    // itemTag3去重升序，并编号
    var itemTag3IndexMapTemp = data.map(_.itemTag3).filter(_!="").distinct().map(_.toInt)
      .sortBy(f=>f,true,1)
      .zipWithIndex().map(f=>(f._1,f._2.toInt))
      .collectAsMap().map(f=>f._1.toString -> f._2)
    // 索引不到itemTag3给默认-1并编号
    if(!itemTag3IndexMapTemp.contains("-1")) itemTag3IndexMapTemp += ("-1" -> itemTag3IndexMapTemp.size)
    val itemTag3IndexMap = itemTag3IndexMapTemp.map(f=>f._1.toInt -> f._2)

    // itemKs1去重升序，并编号
    var itemKs1IndexMapTemp = data.map(_.itemKs1).filter(_!="").distinct().map(_.toInt)
      .sortBy(f=>f,true,1)
      .zipWithIndex().map(f=>(f._1,f._2.toInt))
      .collectAsMap().map(f=>f._1.toString -> f._2)
    // 索引不到itemKs1给默认-1并编号
    if(!itemKs1IndexMapTemp.contains("-1")) itemKs1IndexMapTemp += ("-1" -> itemKs1IndexMapTemp.size)
    val itemKs1IndexMap = itemKs1IndexMapTemp.map(f=>f._1.toInt -> f._2)

    // itemKs2去重升序，并编号
    var itemKs2IndexMapTemp = data.map(_.itemKs2).filter(_!="").distinct().map(_.toInt)
      .sortBy(f=>f,true,1)
      .zipWithIndex().map(f=>(f._1,f._2.toInt))
      .collectAsMap().map(f=>f._1.toString -> f._2)
    // 索引不到itemKs2给默认-1并编号
    if(!itemKs2IndexMapTemp.contains("-1")) itemKs2IndexMapTemp += ("-1" -> itemKs2IndexMapTemp.size)
    val itemKs2IndexMap = itemKs2IndexMapTemp.map(f=>f._1.toInt -> f._2)

    val rdd1 = data.filter(_.itemTexL != "-1")
      .map(f=>{
        f.userRequestTime = DateUtils.getLogHour(f.userRequestTime)
        //处理部分虚假用户年龄和宝宝年龄，统一给默认
        var age = Try(f.userAge.toInt).getOrElse(20)
        var bage = Try(f.userBage.toInt).getOrElse(0)
        if(age < 12){
          age = 20
        }
        if(bage < 0){
          bage = 0
        }
        if(bage > age){
          bage = 0
        }
        if(age < 15 && bage > 0){
          bage = 0
        }
        f.userAge = age.toString
        f.userBage = bage.toString

        f.userBotActCt = getUserBotActCt(f.userColN,f.userRevi,f.userShare,f.userLireN,f.userVreN)
        // 用户历史浏览资讯id、关键词id、一级类目id、二级类目id、三级类目id、关键词1 id、关键词2 id历史浏览记录整合
        val userHListTemp = processUserHList(f.userHList,itemIdIndexMapTemp)
        f.userItemHistory = userHListTemp._1
        f.userKeywordHistory = userHListTemp._2
        f.userTag1History = userHListTemp._3
        f.userTag2History = userHListTemp._4
        f.userTag3History = userHListTemp._5
        f.userKs1History = userHListTemp._6
        f.userKs2History = userHListTemp._7
        f
      })

    val userColumns=Array("label","batchNo","userId",
      "userRequestTime","userOs","userApn","userUa","userMode","userCityLevel","userMarr",
      "userAge","userBage","userAppV","userCliN_Inc","userShoN_Inc","userBotActCt",
      "userTotalTime", "userView2BottomTimes","userEffTimes")

    val userHListColumn=Array("userItemHistory","userKeywordHistory","userTag1History","userTag2History",
      "userTag3History","userKs1History","userKs2History")

    val itemColumns=Array(
      "itemAlgSource","itemTexL","itemKwN","itemTitL","itemTwN","itemImgN","itemSubT","itemSour",
      "itemAuthorScore","itemCreT",
      "itemCliN_Inc","itemShoN_Inc","itemRevi","itemColN","itemShare","itemVreN","itemLireN",
      "itemEffUsers","itemView2BottomTimes","itemTotalTime","itemId","itemKtW","itemTag1","itemTag2","itemTag3","itemKs1","itemKs2")

    // 动态转换rdd=>dataframe,也可用case class反射机制
    val rdd2 = rdd1.map(lb  =>
    RowFactory.create(lb.getValues(userColumns)++lb.getStringValues(userHListColumn)++lb.getValues(itemColumns):_*))

    val df = spark.createDataFrame(rdd2,buildSchema(userColumns++userHListColumn++itemColumns))

    df.cache()
    df.show(10,false)

    // 取出待离散化多列，并制定分割区间大小
    val disColumns=df.columns.filterNot(x=>x.equals("label") || x.equals("batchNo") || x.equals("userId") ||
      x.equals("userItemHistory") || x.equals("userKeywordHistory") || x.equals("userTag1History") ||
      x.equals("userTag2History") || x.equals("userTag3History") || x.equals("userKs1History") || x.equals("userKs2History") ||
      x.equals("itemId") || x.equals("itemKtW") || x.equals("itemTag1") || x.equals("itemTag2") || x.equals("itemTag3") ||
      x.equals("itemKs1")|| x.equals("itemKs2")).map(x=>(x,400))

    // 多列分箱，建立桶索引
    val df2Temp = batchBucketizer(df,disColumns)
    val df2 = df2Temp._1
    df2.show(10,false)

    // DataFrame多列重命名
    val df3=df2
      .withColumnRenamed("userRequestTime_d","userRequestTime")
      .withColumnRenamed("userOs_d","userOs")
      .withColumnRenamed("userApn_d","userApn")
      .withColumnRenamed("userUa_d","userUa")
      .withColumnRenamed("userMode_d","userMode")
      .withColumnRenamed("userCityLevel_d","userCityLevel")
      .withColumnRenamed("userMarr_d","userMarr")
      .withColumnRenamed("userAge_d","userAge")
      .withColumnRenamed("userBage_d","userBage")
      .withColumnRenamed("userAppV_d","userAppV")
      .withColumnRenamed("userCliN_Inc_d","userCliN_Inc")
      .withColumnRenamed("userShoN_Inc_d","userShoN_Inc")
      .withColumnRenamed("userBotActCt_d","userBotActCt")
      .withColumnRenamed("userTotalTime_d","userTotalTime")
      .withColumnRenamed("userView2BottomTimes_d","userView2BottomTimes")
      .withColumnRenamed("userEffTimes_d","userEffTimes")
      .withColumnRenamed("itemAlgSource_d","itemAlgSource")
      .withColumnRenamed("itemTexL_d","itemTexL")
      .withColumnRenamed("itemKwN_d","itemKwN")
      .withColumnRenamed("itemTitL_d","itemTitL")
      .withColumnRenamed("itemTwN_d","itemTwN")
      .withColumnRenamed("itemImgN_d","itemImgN")
      .withColumnRenamed("itemSubT_d","itemSubT")
      .withColumnRenamed("itemSour_d","itemSour")
      .withColumnRenamed("itemAuthorScore_d","itemAuthorScore")
      .withColumnRenamed("itemCreT_d","itemCreT")
      .withColumnRenamed("itemCliN_Inc_d","itemCliN_Inc")
      .withColumnRenamed("itemShoN_Inc_d","itemShoN_Inc")
      .withColumnRenamed("itemRevi_d","itemRevi")
      .withColumnRenamed("itemColN_d","itemColN")
      .withColumnRenamed("itemShare_d","itemShare")
      .withColumnRenamed("itemVreN_d","itemVreN")
      .withColumnRenamed("itemLireN_d","itemLireN")
      .withColumnRenamed("itemEffUsers_d","itemEffUsers")
      .withColumnRenamed("itemView2BottomTimes_d","itemView2BottomTimes")
      .withColumnRenamed("itemTotalTime_d","itemTotalTime")
      .selectExpr(userColumns++userHListColumn++itemColumns: _*)

    df3.show(10,false)

    //对列的值进行排序且按值加唯一索引
    val indexUserColumns=Array(
      "userRequestTime","userOs","userApn","userUa","userMode","userCityLevel","userMarr",
      "userAge","userBage","userAppV","userCliN_Inc","userShoN_Inc","userBotActCt",
      "userTotalTime", "userView2BottomTimes","userEffTimes")

    val indexItemColumns=Array(
      "itemAlgSource","itemTexL","itemKwN","itemTitL","itemTwN","itemImgN","itemSubT","itemSour",
      "itemAuthorScore","itemCreT",
      "itemCliN_Inc","itemShoN_Inc","itemRevi","itemColN","itemShare","itemVreN","itemLireN",
      "itemEffUsers","itemView2BottomTimes","itemTotalTime")

    var userPosition=0
    var itemPosition=0

    val indexUserMap = indexUserColumns.map(f=>{
      var map = df3.select(f).distinct().orderBy(f).rdd.zipWithIndex()
        .map(f=>(f._1.apply(0).toString.toDouble.toInt,f._2.toInt + userPosition)).collectAsMap()
      userPosition += map.size + 1
      map += (-1 -> (userPosition - 1))
      (f,map)
    }).toMap

    val indexItemMap = indexItemColumns.map(f=>{
      var map = df3.select(f).distinct().orderBy(f).rdd.zipWithIndex()
        .map(f=>(f._1.apply(0).toString.toDouble.toInt, f._2.toInt + itemPosition)).collectAsMap()
      itemPosition += map.size + 1
      map += (-1 -> (itemPosition - 1))
      (f,map)
    }).toMap

    import spark.implicits._
    val df4 = df3.map(f=>{
      val userValues = indexUserColumns.map(x=>{
        val map = indexUserMap(x)
        val value = f.getAs[Double](x).toInt
        map.getOrElse(value,map(-1))
      })
      val itemValues = indexItemColumns.map(x=>{
        val map = indexItemMap(x)
        val value = f.getAs[Double](x).toInt
        map.getOrElse(value,map(-1))
      })
      val userItemHistory = Try(f.getAs[String]("userItemHistory").split(",").map(_.toInt)).getOrElse(Array())
      val userKeywordHistory = Try(f.getAs[String]("userKeywordHistory").split(",").map(_.toInt)).getOrElse(Array())
      val userTag1History = Try(f.getAs[String]("userTag1History").split(",").map(_.toInt)).getOrElse(Array())
      val userTag2History = Try(f.getAs[String]("userTag2History").split(",").map(_.toInt)).getOrElse(Array())
      val userTag3History = Try(f.getAs[String]("userTag3History").split(",").map(_.toInt)).getOrElse(Array())
      val userKs1History = Try(f.getAs[String]("userKs1History").split(",").map(_.toInt)).getOrElse(Array())
      val userKs2History = Try(f.getAs[String]("userKs2History").split(",")map(_.toInt)).getOrElse(Array())
      (f.getAs[Double]("label").toInt,f.getAs[Double]("batchNo").toLong,f.getAs[Double]("userId").toLong,userValues,itemValues,
        f.getAs[Double]("itemId").toInt,f.getAs[Double]("itemKtW").toInt,f.getAs[Double]("itemTag1").toInt,
        f.getAs[Double]("itemTag2").toInt,f.getAs[Double]("itemTag3").toInt,f.getAs[Double]("itemKs1").toInt,
        f.getAs[Double]("itemKs2").toInt,
        userItemHistory,userKeywordHistory,userTag1History,userTag2History,userTag3History,userKs1History,userKs2History)
    }).toDF("label","batchNo","userId","user_feature","item_feature","itemId","itemKtW","itemTag1","itemTag2",
      "itemTag3","itemKs1","itemKs2","userItemHistory","userKeywordHistory","userTag1History","userTag2History",
      "userTag3History","userKs1History","userKs2History")

    val df5 = df4.map(f=>{
      val itemId = itemIdIndexMap(f.getAs[Int]("itemId"))
      val keywordId = itemKeywordIndexMap(f.getAs[Int]("itemKtW"))
      val tag1 = itemTag1IndexMap(f.getAs[Int]("itemTag1"))
      val tag2 = itemTag2IndexMap(f.getAs[Int]("itemTag2"))
      val tag3 = itemTag3IndexMap(f.getAs[Int]("itemTag3"))
      val ks1 = itemKs1IndexMap(f.getAs[Int]("itemKs1"))
      val ks2 = itemKs2IndexMap(f.getAs[Int]("itemKs2"))
      val userItemHistory = f.getAs[mutable.WrappedArray[Int]]("userItemHistory").map(x=>itemIdIndexMap.getOrElse(x,"-1"))
      val userKeywordHistory = f.getAs[mutable.WrappedArray[Int]]("userKeywordHistory").map(x=>itemKeywordIndexMap.getOrElse(x,"-1"))
      val userTag1History = f.getAs[mutable.WrappedArray[Int]]("userTag1History").map(x=>itemTag1IndexMap.getOrElse(x,"-1"))
      val userTag2History = f.getAs[mutable.WrappedArray[Int]]("userTag2History").map(x=>itemTag2IndexMap.getOrElse(x,"-1"))
      val userTag3History = f.getAs[mutable.WrappedArray[Int]]("userTag3History").map(x=>itemTag3IndexMap.getOrElse(x,"-1"))
      val userKs1History = f.getAs[mutable.WrappedArray[Int]]("userKs1History").map(x=>itemKs1IndexMap.getOrElse(x,"-1"))
      val userKs2History = f.getAs[mutable.WrappedArray[Int]]("userKs2History").map(x=>itemKs2IndexMap.getOrElse(x,"-1"))
      (f.getAs[Int]("label"),f.getAs[Long]("batchNo"),f.getAs[Long]("userId"),
        f.getAs[mutable.WrappedArray[Int]]("user_feature").mkString(","),
        f.getAs[mutable.WrappedArray[Int]]("item_feature").mkString(","),
        itemId,keywordId,tag1,tag2,tag3,ks1,ks2,userItemHistory.mkString(","),userKeywordHistory.mkString(","),
        userTag1History.mkString(","),userTag2History.mkString(","),userTag3History.mkString(","),
        userKs1History.mkString(","),userKs2History.mkString(","))
    }).toDF("label","batchNo","userId","user_feature","item_feature","itemId","itemKtW","itemTag1","itemTag2","itemTag3",
      "itemKs1","itemKs2","userItemHistory","userKeywordHistory","userTag1History","userTag2History","userTag3History",
      "userKs1History","userKs2History")

    df5.show(20,false)
    val outPath = "/opt/data/feeds/features/"+System.currentTimeMillis()
    df5.repartition(1).orderBy("batchNo").repartition(1).write.option("header","true").csv(outPath)

    //把离散用的数组和列名存下来
    spark.sparkContext.parallelize(df2Temp._2.map(f=>(f._1,f._2.mkString(","))),1).saveAsTextFile(outPath + "/disArray")
    spark.sparkContext.parallelize(disColumns.map(_._1),1).saveAsTextFile(outPath + "/columns")

    var indexMap = indexUserMap ++ indexItemMap
    indexMap += "itemId" -> itemIdIndexMap
    indexMap += "itemKtW" -> itemKeywordIndexMap
    indexMap += "itemTag1" -> itemTag1IndexMap
    indexMap += "itemTag2" -> itemTag2IndexMap
    indexMap += "itemTag3" -> itemTag3IndexMap
    indexMap += "itemKs1" -> itemKs1IndexMap
    indexMap += "itemKs2" -> itemKs2IndexMap

    //把特征索引值存下来
    val indexJson = indexMap.map(f=>f._1->f._2.map(x=>x._1.toString -> x._2)).map(f=>f._1->JSONObject(f._2.toMap))
    spark.sparkContext.parallelize(Array(JSONObject(indexJson).toString()),1)

    var dict:Map[String,Int] = Map()
    dict += ("user_features_num" -> indexUserMap.size)
    dict += ("item_features_num" -> indexItemMap.size)
    dict += ("user_features_dim" -> userPosition)
    dict += ("item_features_dim" -> itemPosition)
    dict += ("item_count" -> itemIdIndexMap.size)
    dict += ("keyword_count" -> itemKeywordIndexMap.size)
    dict += ("tag1_count" -> itemTag1IndexMap.size)
    dict += ("tag2_count" -> itemTag2IndexMap.size)
    dict += ("tag3_count" -> itemTag3IndexMap.size)
    dict += ("ks1_count" -> itemKs1IndexMap.size)
    dict += ("ks2_count" -> itemKs2IndexMap.size)
    val jsonString = JSONObject(dict)
    spark.sparkContext.parallelize(Array(jsonString.toString()),1).saveAsTextFile(outPath + "/countDict")

    spark.stop()
  }

  /**
    * 加载label、user、item对应的logs，合并后instance
    * @param spark
    * @param path
    * @return RDD[T]
    */
  private def log2Features(spark:SparkSession, path:String):RDD[CommonLabelPoint2] = {
    //按照logType分类加载数据
    val data = spark.sparkContext
      .textFile(path)
      .map(f=>CommonLabelPoint2.toObjectForDIN(f)).filter(_!=null)

    //实例化label部分
    val labelData = data
      //筛选label型点击或曝光
      .filter(f=>f.logType == "1"&&(f.label=="0" || f.label=="1"))
      //过滤重复点击或曝光
      .map(f=>((f.userId,f.itemId,f.label.toInt),f))
      .groupBy(_._1)
      .flatMap(f=>f._2.take(1)).map(_._2)
      .map(f=>((f.userId,f.itemId),f.label.toInt,f))
      .groupBy(_._1)
      .filter(f=>f._2.size==1 || f._2.size==2)
      .map(f=>{
        if(f._2.size == 1){
          f._2.map(_._3).toArray.apply(0)
        }else{
          if(f._2.filter(_._2 == 1).map(_._3).nonEmpty) f._2.filter(_._2 == 1).map(_._3).toArray.apply(0) else null
        }
      }).filter(_!= null)

    //label和users两部分join
    val labelUsers = labelData
      .map(f=>((f.batchNo,f.userId),f))
      .join(data.filter(_.logType=="2").map(f=>((f.batchNo,f.userId),f)))
      .map(f=>CommonLabelPoint2.copyUser2LbForDIN(f._2._1,f._2._2))

    //labelUsers和item两部分join
    val labelUserItems = labelUsers
      .map(f=>((f.batchNo,f.itemDataType,f.itemId),f))
      .join(data.filter(_.logType == "3").map(f=>((f.batchNo,f.itemDataType,f.itemId),f)))
      .map(f=>CommonLabelPoint2.copyItem2LbForDIN(f._2._1,f._2._2))

    labelUserItems
  }

  def getUserBotActCt(userColN:String,userRevi:String,userShare:String,userLireN:String,userVreN:String) = {
    var result = 0
    if(StringUtils.isNoneEmpty(userColN) && !"-1".equals(userColN)){
      result += userColN.toInt
    }
    if(StringUtils.isNoneEmpty(userRevi) && !"-1".equals(userRevi)){
      result += userRevi.toInt
    }
    if(StringUtils.isNoneEmpty(userShare) && !"-1".equals(userShare)){
      result += userShare.toInt
    }
    if(StringUtils.isNoneEmpty(userLireN) && !"-1".equals(userLireN)){
      result += userLireN.toInt
    }
    if(StringUtils.isNoneEmpty(userVreN) && !"-1".equals(userVreN)){
      result += userVreN.toInt
    }
    result.toString
  }

  /**
    * 历史浏览记录整合
    * @param userHList
    * @param itemIdIndexMap
    * @return
    */
  private def processUserHList(userHList:String,itemIdIndexMap:scala.collection.Map[String,Int]) = {
    val itemIdArrayBuffer = ArrayBuffer[String]()
    val keywordIdArrayBuffer = ArrayBuffer[String]()
    val tag1ArrayBuffer = ArrayBuffer[String]()
    val tag2ArrayBuffer = ArrayBuffer[String]()
    val tag3ArrayBuffer = ArrayBuffer[String]()
    val ks1ArrayBuffer = ArrayBuffer[String]()
    val ks2ArrayBuffer = ArrayBuffer[String]()
    val ar = userHList.split(",").reverse

    ar.foreach(f=>{
      val fs = f.split("_")
      if(fs.length > 9 && fs(1) == "2"){
        val itemId = fs.applyOrElse(0,"-1").toString
        val keywordId = Try(fs.apply(3)).getOrElse("-1")
        val tag1Id = Try(fs.apply(5)).getOrElse("-1")
        val tag2Id = Try(fs.apply(6)).getOrElse("-1")
        val tag3Id = Try(fs.apply(7)).getOrElse("-1")
        val ks1Id = Try(fs.apply(8)).getOrElse("-1")
        val ks2Id = Try(fs.apply(9)).getOrElse("-1")
        itemIdArrayBuffer.append(itemId)
        keywordIdArrayBuffer.append(keywordId)
        tag1ArrayBuffer.append(tag1Id)
        tag2ArrayBuffer.append(tag2Id)
        tag3ArrayBuffer.append(tag3Id)
        ks1ArrayBuffer.append(ks1Id)
        ks2ArrayBuffer.append(ks2Id)
      }
    })

    (itemIdArrayBuffer.mkString(","),keywordIdArrayBuffer.mkString(","),tag1ArrayBuffer.mkString(","),tag2ArrayBuffer.mkString(","),tag3ArrayBuffer.mkString(","),ks1ArrayBuffer.mkString(","),ks2ArrayBuffer.mkString(","))
  }

  private def buildSchema(columns:Array[String])={
    types.StructType(columns.map(f=>{
      if(f=="userItemHistory" || f=="userKeywordHistory" || f=="userTag1History" || f=="userTag2History"  ||
        f=="userTag3History" || f=="userKs1History" || f=="userKs2History"){
        StructField(f,StringType,true)
      }
      else StructField(f,DoubleType,true)
    }))
  }

  /**
    * 对指定多列分箱，并建立桶索引，将连续数据转化为离散数据，并drop原始列
    * @param data
    * @param discreteColumns
    * @param isDrop
    * @return (DataFrame,Array)
    */
  private def batchBucketizer(data:DataFrame,discreteColumns:Array[(String,Int)],isDrop:Boolean=true) = {
    val newColumns = ArrayBuffer[(String,Array[Double])]()
    discreteColumns.foreach(columns=>{
      data.stat
      // 计算dataframe数值列的近似分位数
      val splits = data.stat.approxQuantile(columns._1,(0.0 to 1.0 by 1.0 / columns._2).toArray, 0.001)
      splits(0) = Double.NegativeInfinity
      splits(splits.length - 1) = Double.PositiveInfinity
      val distinctSplits = splits.distinct.sorted
      newColumns += ((columns._1,distinctSplits))
    })
    newColumns.foreach(f=>println(f._1,f._2.mkString(",")))

    // 每列分箱，将连续数值离散化到指定分位数区间中，封装到pipeline中
    val bucketizers:Array[PipelineStage] = newColumns.toArray.map(
      name => new Bucketizer()
        .setInputCol(name._1)
        .setOutputCol(s"${name._1}_d")
        .setSplits(name._2)
    )
    // 原始数据转化为桶索引
    val dropColumns = discreteColumns.map(_._1)
    val pipeline = new Pipeline().setStages(bucketizers)
    val df = pipeline.fit(data).transform(data)
    if(isDrop) (df.drop(dropColumns: _*),newColumns.toArray) else (df,newColumns.toArray)
  }
}
