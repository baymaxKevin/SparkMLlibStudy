package com.sparkMLlibStudy.features

import java.sql.{Connection, DriverManager}

import com.sparkStudy.utils.DateUtils
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.parsing.json.JSONObject


object DINVideoFeaturesProcess {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("DINVideoFeaturesProcess").getOrCreate()
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.kryoserializer.buffer.max","512m")
    var date=DateUtils.getDay(-1)
    var numPartitions=4
    if(args.length>0) numPartitions=args(0).toInt
    if(args.length>1) date=args(1)
    val saveTime=System.currentTimeMillis()
    val dataPath="/opt/data/feeds/logs/sample"
    val modelOutPath="/opt/data/feeds/features/"+System.currentTimeMillis()

    //1读取特征日志并转成对象
    val dataTemp=loadData2(spark,dataPath).repartition(numPartitions)

    val data=dataTemp.filter(_.itemTexL!="-1")
      .map(x=>{
        x.userRequestTime=DateUtils.getLogHour(x.userRequestTime)
        var age=Try(x.userAge.toInt).getOrElse(20)
        var bage=Try(x.userBage.toInt).getOrElse(0)
        if(age < 12){
          age=20
        }
        if(bage<0){
          bage=0
        }
        if(bage > age){
          bage=0
        }
        if(age < 15 && bage > 0 ){
          bage=0
        }
        x.userAge=age.toString
        x.userBage=bage.toString
        x.userBotActCt=FeaturesProcess.getUserBotActCt(x.userColN,x.userRevi,x.userShare,x.userLireN,x.userVreN)
        val userHListTemp=processUserHList(x.userHList)
        x.userItemHistory=userHListTemp._1
        x.userCateHistory=userHListTemp._2
        x.userKeywordHistory=userHListTemp._3
        x.userKeyword2History=userHListTemp._4
        x.userTag1History=userHListTemp._5
        x.userTag2History=userHListTemp._6
        x.userTag3History=userHListTemp._7
        x.userKs1History=userHListTemp._8
        x.userKs2History=userHListTemp._9
        x
      })
    data.filter(f=>f.userId=="116199332" ||f.userId =="126413717" || f.userId =="188420966").take(10).foreach(x=>println(x.toString))


    val userColumns=Array("label","batchNo","userId",
      "userRequestTime","userOs","userApn","userUa","userMode","userCityLevel","userMarr",
      "userAge","userBage","userAppV","userCliN_Inc","userShoN_Inc","userBotActCt",
      "userTotalTime", "userView2BottomTimes","userEffTimes")

    val userHListColumn=Array("userCateHistory","userKeywordHistory","userKeyword2History"
      ,"userTag1History","userTag2History","userTag3History","userKs1History","userKs2History")

    val itemColumns=Array(
      "itemAlgSource","itemTexL","itemKwN","itemTitL","itemTwN","itemImgN","itemSubT","itemSour",
      "itemAuthorScore","itemCreT",
      "itemCliN_Inc","itemShoN_Inc","itemRevi","itemColN","itemShare","itemVreN","itemLireN",
      "itemEffUsers","itemView2BottomTimes","itemTotalTime","itemId",
      "itemTtP","itemKtW","itemKtW2","itemTag1","itemTag2","itemTag3","itemKs1","itemKs2")


    val data2=data.map(lb=>
      RowFactory.create(lb.getValues(userColumns)++lb.getStringValues(userHListColumn)++lb.getValues(itemColumns):_*))

    val df = spark.createDataFrame(data2,buildSchema(userColumns++userHListColumn++itemColumns))
    df.cache()
    df.show(10,false)

    val disColumns=df.columns.filterNot(x=>x.equals("label") || x.equals("batchNo") || x.equals("userId") ||
      x.equals("userCateHistory") ||
      x.equals("userKeywordHistory") || x.equals("userKeyword2History") ||
      x.equals("userTag1History") || x.equals("userTag2History") || x.equals("userTag3History") ||
      x.equals("userKs1History") || x.equals("userKs2History") || x.equals("itemId") ||
      x.equals("itemTtP") || x.equals("itemKtW") || x.equals("itemKtW2") || x.equals("itemTag1")
      || x.equals("itemTag2")|| x.equals("itemTag3")|| x.equals("itemKs1") || x.equals("itemKs2")).map(x=>(x,400))
    val df2Temp=batchBucketizer(df,disColumns)
    val df2=df2Temp._1
    df2.show(10,false)

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

    val indexUserMap=indexUserColumns.map(x=>{
      var map=df3.select(x).distinct().orderBy(x).rdd.zipWithIndex().map(x=> (x._1.apply(0).toString.toDouble.toInt,x._2.toInt+userPosition)).collectAsMap()
      userPosition +=map.size+1
      map += (-1 -> (userPosition-1))
      (x,map)
    }).toMap

    val indexItemMap=indexItemColumns.map(x=>{
      var map=df3.select(x).distinct().orderBy(x).rdd.zipWithIndex().map(x=> (x._1.apply(0).toString.toDouble.toInt,x._2.toInt+itemPosition)).collectAsMap()
      itemPosition +=map.size+1
      map += (-1 -> (itemPosition-1))
      (x,map)
    }).toMap

    import spark.implicits._
    val df4=df3.map(x=>{
      val userValues=indexUserColumns.map(y=>{
        val map=indexUserMap(y)
        val value = x.getAs[Double](y).toInt
        map.getOrElse(value,map(-1))
      })
      val itemValues=indexItemColumns.map(y=>{
        val map=indexItemMap(y)
        val value=x.getAs[Double](y).toInt
        map.getOrElse(value,map(-1))
      })
      val label=x.getAs[Double]("label").toInt
      val itemId=x.getAs[Double]("itemId").toInt
      val userItemHistory=Try(x.getAs[String]("userItemHistory").split(",").map(_.toInt)).getOrElse(Array())
      var itemIdIndex= -1
      if(userItemHistory.nonEmpty) itemIdIndex=userItemHistory.indexOf(itemId)
      //记录训练日志的任务和记录用户历史浏览的任务时间不同步,导致当前正样本的item可能出现在用户历史浏览的最近一条训练时出现穿越问题
      if(label==1 && itemIdIndex > -1) {
        val userCateHistory = Try(x.getAs[String]("userCateHistory").split(",").map(_.toInt)).getOrElse(Array())
        val userKeywordHistory = Try(x.getAs[String]("userKeywordHistory").split(",").map(_.toInt)).getOrElse(Array())
        val userKeyword2History = Try(x.getAs[String]("userKeyword2History").split(",").map(_.toInt)).getOrElse(Array())
        val userTag1History = Try(x.getAs[String]("userTag1History").split(",").map(_.toInt)).getOrElse(Array())
        val userTag2History = Try(x.getAs[String]("userTag2History").split(",").map(_.toInt)).getOrElse(Array())
        val userTag3History = Try(x.getAs[String]("userTag3History").split(",").map(_.toInt)).getOrElse(Array())
        val userKs1History = Try(x.getAs[String]("userKs1History").split(",").map(_.toInt)).getOrElse(Array())
        val userKs2History = Try(x.getAs[String]("userKs2History").split(",").map(_.toInt)).getOrElse(Array())
        (label, x.getAs[Double]("batchNo").toLong, x.getAs[Double]("userId").toLong, userValues, itemValues,
          x.getAs[Double]("itemTtP").toInt, x.getAs[Double]("itemKtW").toInt, x.getAs[Double]("itemKtW2").toInt,
          x.getAs[Double]("itemTag1").toInt, x.getAs[Double]("itemTag2").toInt, x.getAs[Double]("itemTag3").toInt,
          x.getAs[Double]("itemKs1").toInt, x.getAs[Double]("itemKs2").toInt,
          userCateHistory.take(itemIdIndex), userKeywordHistory.take(itemIdIndex), userKeyword2History.take(itemIdIndex),
          userTag1History.take(itemIdIndex), userTag2History.take(itemIdIndex), userTag3History.take(itemIdIndex),
          userKs1History.take(itemIdIndex), userKs2History.take(itemIdIndex))
      }else{
        val userCateHistory = Try(x.getAs[String]("userCateHistory").split(",").map(_.toInt)).getOrElse(Array())
        val userKeywordHistory = Try(x.getAs[String]("userKeywordHistory").split(",").map(_.toInt)).getOrElse(Array())
        val userKeyword2History = Try(x.getAs[String]("userKeyword2History").split(",").map(_.toInt)).getOrElse(Array())
        val userTag1History = Try(x.getAs[String]("userTag1History").split(",").map(_.toInt)).getOrElse(Array())
        val userTag2History = Try(x.getAs[String]("userTag2History").split(",").map(_.toInt)).getOrElse(Array())
        val userTag3History = Try(x.getAs[String]("userTag3History").split(",").map(_.toInt)).getOrElse(Array())
        val userKs1History = Try(x.getAs[String]("userKs1History").split(",").map(_.toInt)).getOrElse(Array())
        val userKs2History = Try(x.getAs[String]("userKs2History").split(",").map(_.toInt)).getOrElse(Array())
        (label, x.getAs[Double]("batchNo").toLong, x.getAs[Double]("userId").toLong, userValues, itemValues,
          x.getAs[Double]("itemTtP").toInt, x.getAs[Double]("itemKtW").toInt, x.getAs[Double]("itemKtW2").toInt,
          x.getAs[Double]("itemTag1").toInt, x.getAs[Double]("itemTag2").toInt, x.getAs[Double]("itemTag3").toInt,
          x.getAs[Double]("itemKs1").toInt, x.getAs[Double]("itemKs2").toInt,
          userCateHistory, userKeywordHistory, userKeyword2History, userTag1History, userTag2History, userTag3History, userKs1History, userKs2History)
      }
    }).toDF("label","batchNo","userId","user_feature","item_feature","itemTtP","itemKtW","itemKtW2",
      "itemTag1","itemTag2","itemTag3","itemKs1","itemKs2",
      "userCateHistory","userKeywordHistory","userKeyword2History",
      "userTag1History","userTag2History","userTag3History","userKs1History","userKs2History")


    //    var itemIdIndexMap=df4.select("itemId").rdd.map(x=>x.getAs[Int](0))
    //      .union(df4.select("userItemHistory").rdd.flatMap(x=>x.getAs[mutable.WrappedArray[Int]](0))).distinct()
    //      .sortBy(x=>x,true,1).zipWithIndex().map(x=>(x._1,x._2.toInt)).collectAsMap()
    //    if(!itemIdIndexMap.contains(-1)) itemIdIndexMap +=(-1 -> itemIdIndexMap.size)

    var itemCateIndexMap=df4.select("itemTtP").rdd.map(x=>x.getAs[Int](0))
      .union(df4.select("userCateHistory").rdd.flatMap(x=>x.getAs[mutable.WrappedArray[Int]](0))).distinct()
      .sortBy(x=>x,true,1).zipWithIndex().map(x=>(x._1,x._2.toInt)).collectAsMap()
    if(!itemCateIndexMap.contains(-1)) itemCateIndexMap +=(-1 -> itemCateIndexMap.size)

    var itemKeywordIndexMap=df4.select("itemKtW").rdd.map(x=>x.getAs[Int](0))
      .union(df4.select("userKeywordHistory").rdd.flatMap(x=>x.getAs[mutable.WrappedArray[Int]](0))).distinct()
      .sortBy(x=>x,true,1).zipWithIndex().map(x=>(x._1,x._2.toInt)).collectAsMap()
    if(!itemKeywordIndexMap.contains(-1)) itemKeywordIndexMap +=(-1 -> itemKeywordIndexMap.size)

    var itemKeyword2IndexMap=df4.select("itemKtW2").rdd.map(x=>x.getAs[Int](0))
      .union(df4.select("userKeyword2History").rdd.flatMap(x=>x.getAs[mutable.WrappedArray[Int]](0))).distinct()
      .sortBy(x=>x,true,1).zipWithIndex().map(x=>(x._1,x._2.toInt)).collectAsMap()
    if(!itemKeyword2IndexMap.contains(-1)) itemKeyword2IndexMap +=(-1 -> itemKeyword2IndexMap.size)

    var itemTag1IndexMap=df4.select("itemTag1").rdd.map(x=>x.getAs[Int](0))
      .union(df4.select("userTag1History").rdd.flatMap(x=>x.getAs[mutable.WrappedArray[Int]](0))).distinct()
      .sortBy(x=>x,true,1).zipWithIndex().map(x=>(x._1,x._2.toInt)).collectAsMap()
    if(!itemTag1IndexMap.contains(-1)) itemTag1IndexMap +=(-1 -> itemTag1IndexMap.size)

    var itemTag2IndexMap=df4.select("itemTag2").rdd.map(x=>x.getAs[Int](0))
      .union(df4.select("userTag2History").rdd.flatMap(x=>x.getAs[mutable.WrappedArray[Int]](0))).distinct()
      .sortBy(x=>x,true,1).zipWithIndex().map(x=>(x._1,x._2.toInt)).collectAsMap()
    if(!itemTag2IndexMap.contains(-1)) itemTag2IndexMap +=(-1 -> itemTag2IndexMap.size)

    var itemTag3IndexMap=df4.select("itemTag3").rdd.map(x=>x.getAs[Int](0))
      .union(df4.select("userTag3History").rdd.flatMap(x=>x.getAs[mutable.WrappedArray[Int]](0))).distinct()
      .sortBy(x=>x,true,1).zipWithIndex().map(x=>(x._1,x._2.toInt)).collectAsMap()
    if(!itemTag3IndexMap.contains(-1)) itemTag3IndexMap +=(-1 -> itemTag3IndexMap.size)

    var itemKs1IndexMap=df4.select("itemKs1").rdd.map(x=>x.getAs[Int](0))
      .union(df4.select("userKs1History").rdd.flatMap(x=>x.getAs[mutable.WrappedArray[Int]](0))).distinct()
      .sortBy(x=>x,true,1).zipWithIndex().map(x=>(x._1,x._2.toInt)).collectAsMap()
    if(!itemKs1IndexMap.contains(-1)) itemKs1IndexMap +=(-1 -> itemKs1IndexMap.size)

    var itemKs2IndexMap=df4.select("itemKs2").rdd.map(x=>x.getAs[Int](0))
      .union(df4.select("userKs2History").rdd.flatMap(x=>x.getAs[mutable.WrappedArray[Int]](0))).distinct()
      .sortBy(x=>x,true,1).zipWithIndex().map(x=>(x._1,x._2.toInt)).collectAsMap()
    if(!itemKs2IndexMap.contains(-1)) itemKs2IndexMap +=(-1 -> itemKs2IndexMap.size)


    val df5=df4.map(x=>{
      val cateId=itemCateIndexMap(x.getAs[Int]("itemTtP"))
      val keywordId=itemKeywordIndexMap(x.getAs[Int]("itemKtW"))
      val itemKtW2=itemKeyword2IndexMap(x.getAs[Int]("itemKtW2"))
      val itemTag1=itemTag1IndexMap(x.getAs[Int]("itemTag1"))
      val itemTag2=itemTag2IndexMap(x.getAs[Int]("itemTag2"))
      val itemTag3=itemTag3IndexMap(x.getAs[Int]("itemTag3"))
      val itemKs1=itemKs1IndexMap(x.getAs[Int]("itemKs1"))
      val itemKs2=itemKs2IndexMap(x.getAs[Int]("itemKs2"))
      //val userItemHistory=x.getAs[mutable.WrappedArray[Int]]("userItemHistory").map(y=>itemIdIndexMap(y))
      val userCateHistory=x.getAs[mutable.WrappedArray[Int]]("userCateHistory").map(y=>itemCateIndexMap.getOrElse(y, "-1"))
      val userKeywordHistory=x.getAs[mutable.WrappedArray[Int]]("userKeywordHistory").map(y=>itemKeywordIndexMap.getOrElse(y, "-1"))
      val userKeyword2History=x.getAs[mutable.WrappedArray[Int]]("userKeyword2History").map(y=>itemKeyword2IndexMap.getOrElse(y, "-1"))
      val userTag1History=x.getAs[mutable.WrappedArray[Int]]("userTag1History").map(y=>itemTag1IndexMap.getOrElse(y, "-1"))
      val userTag2History=x.getAs[mutable.WrappedArray[Int]]("userTag2History").map(y=>itemTag2IndexMap.getOrElse(y, "-1"))
      val userTag3History=x.getAs[mutable.WrappedArray[Int]]("userTag3History").map(y=>itemTag3IndexMap.getOrElse(y, "-1"))
      val userKs1History=x.getAs[mutable.WrappedArray[Int]]("userKs1History").map(y=>itemKs1IndexMap.getOrElse(y, "-1"))
      val userKs2History=x.getAs[mutable.WrappedArray[Int]]("userKs2History").map(y=>itemKs2IndexMap.getOrElse(y, "-1"))
      (x.getAs[Int]("label"),x.getAs[Long]("batchNo"),x.getAs[Long]("userId"),
        x.getAs[mutable.WrappedArray[Int]]("user_feature").mkString(","),
        x.getAs[mutable.WrappedArray[Int]]("item_feature").mkString(","),
        cateId,keywordId,itemKtW2,itemTag1,itemTag2,itemTag3,itemKs1,itemKs2,
        userCateHistory.mkString(","),
        userKeywordHistory.mkString(","),userKeyword2History.mkString(","),
        userTag1History.mkString(","),userTag2History.mkString(","),userTag3History.mkString(","),
        userKs1History.mkString(","),userKs2History.mkString(","))
    }).toDF("label","batchNo","userId","user_feature","item_feature","itemTtP","itemKtW","itemKtW2",
      "itemTag1","itemTag2","itemTag3","itemKs1","itemKs2",
      "userCateHistory","userKeywordHistory","userKeyword2History",
      "userTag1History","userTag2History","userTag3History","userKs1History","userKs2History")


    df5.show(20,false)

    df5.filter($"userId" isin(116199332,126413717,188420966)).show(20,false)

    df5.repartition(1).orderBy("batchNo").repartition(1).write.option("header","true").csv(modelOutPath)

    //把离散用的数组和列名存下来
    spark.sparkContext.parallelize(df2Temp._2.map(x=>(x._1,x._2.mkString(","))),1).saveAsTextFile(modelOutPath+"/disArray")
    spark.sparkContext.parallelize(disColumns.map(_._1),1).saveAsTextFile(modelOutPath+"/columns")

    var indexMap=indexUserMap ++ indexItemMap
    //indexMap +="itemId" -> itemIdIndexMap
    indexMap +="itemTtP" -> itemCateIndexMap
    indexMap +="itemKtW" -> itemKeywordIndexMap
    indexMap +="itemKtW2" -> itemKeyword2IndexMap
    indexMap +="itemTag1" -> itemTag1IndexMap
    indexMap +="itemTag2" -> itemTag2IndexMap
    indexMap +="itemTag3" -> itemTag3IndexMap
    indexMap +="itemKs1" -> itemKs1IndexMap
    indexMap +="itemKs2" -> itemKs2IndexMap


    //把特征的索引值存下来
    val indexJson=indexMap.map(x=> x._1 -> x._2.map(y=> y._1.toString -> y._2)).map(x=> x._1 -> JSONObject(x._2.toMap))
    spark.sparkContext.parallelize(Array(JSONObject(indexJson).toString()),1).saveAsTextFile(modelOutPath+"/indexMap")

    var count_dict:Map[String,Int] = Map()
    count_dict += ("user_features_num" -> indexUserMap.size)
    count_dict += ("item_features_num" -> indexItemMap.size)
    count_dict += ("user_features_dim" -> userPosition)
    count_dict += ("item_features_dim" -> itemPosition)
    //count_dict += ("item_count" -> itemIdIndexMap.size)
    count_dict += ("cate_count" -> itemCateIndexMap.size)
    count_dict += ("keyword_count" -> itemKeywordIndexMap.size)
    count_dict += ("keyword2_count" -> itemKeyword2IndexMap.size)
    count_dict += ("tag1_count" -> itemTag1IndexMap.size)
    count_dict += ("tag2_count" -> itemTag2IndexMap.size)
    count_dict += ("tag3_count" -> itemTag3IndexMap.size)
    count_dict += ("ks1_count" -> itemKs1IndexMap.size)
    count_dict += ("ks2_count" -> itemKs2IndexMap.size)
    val jsonString = JSONObject(count_dict)
    spark.sparkContext.parallelize(Array(jsonString.toString()),1).saveAsTextFile(modelOutPath+"/countDict")

    spark.stop()
  }




  private def loadData2(ss:SparkSession,path:String):RDD[CommonLabelPoint2]={
    //1读取特征日志并转成日志对象
    val data=ss.sparkContext.textFile(path).map(x=>CommonLabelPoint2.toObjectForDIN(x)).filter(_!=null)
    data.filter(_.logType=="2").foreach(f=>println(f.label2StringDIN + ";" + f.user2StringDIN + ";" + f.item2StringDIN))
    println("data:"+data.count())

    val labelData=data.filter(_.logType=="1").filter(x=>x.label=="0" || x.label=="1")
      .map(x=>((x.userId,x.itemId,x.label.toInt),x))
      .groupBy(_._1)
      .flatMap(x=>x._2.take(1)).map(_._2)//去重复点击或曝光
      .map(x=>((x.userId,x.itemId),x.label.toInt,x)).groupBy(_._1).filter(x=>x._2.size==1 || x._2.size==2)
      .map(x=>{

        if(x._2.size==1){
          x._2.map(_._3).toArray.apply(0)
        }
        else{
          val isClick=if(x._2.filter(_._2==1).map(_._3).nonEmpty) x._2.filter(_._2==1).map(_._3).toArray.apply(0) else null
          isClick
        }

      }).filter(_!=null)
    println("label 2 string:::")
    labelData.foreach(f=>println(f.label2String))
    println("labelData:" + labelData.count())

    val labelUsers=labelData.map(x=>((x.batchNo,x.userId),x)).join(data.filter(_.logType=="2").map(x=>((x.batchNo,x.userId),x)))
      .map(x=>CommonLabelPoint2.copyUser2LbForDIN(x._2._1,x._2._2))
    println("labelUsers:" + labelUsers.count())

    val labelUserItems=labelUsers.map(x=>((x.batchNo,x.itemDataType,x.itemId),x)).join(data.filter(_.logType=="3").map(x=>((x.batchNo,x.itemDataType,x.itemId),x)))
      .map(x=>CommonLabelPoint2.copyItem2LbForDIN(x._2._1,x._2._2))
    println("labelUserItems:" + labelUserItems.count())
    labelUserItems
  }


  private def buildSchema(columns:Array[String])={
    StructType(columns.map(f=>{
      if(f=="userItemHistory" || f=="userCateHistory" || f=="userKeywordHistory" || f=="userKeyword2History" ||
        f=="userTag1History" || f=="userTag2History" || f=="userTag3History" || f=="userKs1History" || f=="userKs2History"){
        StructField(f,StringType,true)
      }
      else StructField(f,DoubleType,true)
    }))
  }



  private def batchBucketizer(data:DataFrame,discreteColumns:Array[(String,Int)],isDrop:Boolean=true)= {
    val newColumns = ArrayBuffer[(String, Array[Double])]()
    discreteColumns.foreach(column => {
      val splits = data.stat.approxQuantile(column._1, (0.0 to 1.0 by 1.0 / column._2).toArray, 0.001)
      splits(0) = Double.NegativeInfinity
      splits(splits.length - 1) = Double.PositiveInfinity
      val distinctSplits = splits.distinct.sorted
      newColumns += ((column._1, distinctSplits))
    })
    newColumns.foreach(x => println(x._1, x._2.mkString(",")))

    val bucketizers: Array[PipelineStage] = newColumns.toArray.map(
      name => new Bucketizer()
        .setInputCol(name._1)
        .setOutputCol(s"${name._1}_d")
        .setSplits(name._2)
      //.setHandleInvalid("skip")
    )
    val dropColumns = discreteColumns.map(_._1)
    val pipeline = new Pipeline().setStages(bucketizers)
    val df = pipeline.fit(data).transform(data)
    if (isDrop) (df.drop(dropColumns: _*), newColumns.toArray) else (df, newColumns.toArray)
  }

  private def processUserHList(userHList:String)={
    val itemIdArrBuffer=ArrayBuffer[String]()
    val cateIdArrBuffer=ArrayBuffer[String]()
    val keywordIdArrBuffer=ArrayBuffer[String]()
    val keywordId2ArrBuffer=ArrayBuffer[String]()
    val tag1ArrBuffer=ArrayBuffer[String]()
    val tag2ArrBuffer=ArrayBuffer[String]()
    val tag3ArrBuffer=ArrayBuffer[String]()
    val ks1ArrBuffer=ArrayBuffer[String]()
    val ks2ArrBuffer=ArrayBuffer[String]()
    val arr=userHList.split(",").reverse
    arr.foreach(x=>{
      val temp=x.split("_")
      if(temp.length>9){
        val itemId=temp.applyOrElse(0,"-1").toString
        val cateId=Try(temp.apply(2)).getOrElse("-1")
        val keywordId=Try(temp.apply(3)).getOrElse("-1")
        val keyword2Id=Try(temp.apply(4)).getOrElse("-1")
        val tag1Id=Try(temp.apply(5)).getOrElse("-1")
        val tag2Id=Try(temp.apply(6)).getOrElse("-1")
        val tag3Id=Try(temp.apply(7)).getOrElse("-1")
        val ks1Id=Try(temp.apply(8)).getOrElse("-1")
        val ks2Id=Try(temp.apply(9)).getOrElse("-1")
        itemIdArrBuffer.append(itemId)
        cateIdArrBuffer.append(cateId)
        keywordIdArrBuffer.append(keywordId)
        keywordId2ArrBuffer.append(keyword2Id)
        tag1ArrBuffer.append(tag1Id)
        tag2ArrBuffer.append(tag2Id)
        tag3ArrBuffer.append(tag3Id)
        ks1ArrBuffer.append(ks1Id)
        ks2ArrBuffer.append(ks2Id)
      }
    })
    (itemIdArrBuffer.mkString(","),cateIdArrBuffer.mkString(",")
      ,keywordIdArrBuffer.mkString(","),keywordId2ArrBuffer.mkString(",")
      ,tag1ArrBuffer.mkString(","),tag2ArrBuffer.mkString(","),tag3ArrBuffer.mkString(",")
      ,ks1ArrBuffer.mkString(","),ks2ArrBuffer.mkString(","))
  }

}
