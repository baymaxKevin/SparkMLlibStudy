package com.sparkStudy.chapter

import com.alibaba.fastjson.JSON
import com.sparkStudy.utils.HBaseUtils1x
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Spark Streaming是核心SparkAPI的扩展，可实现实时数据流的可扩展，高吞吐量，容错流处理;
  * 可以从许多来源（如Kafka，Flume，Kinesis或TCP套接字）中获取数据，
  * 并且可以使用以高级函数（如map，reduce，join和window）表示的复杂算法进行处理;
  * 最后，处理后的数据可以推送到文件系统，数据库和实时仪表板。
  */
object SparkStreaming {
  def main(args: Array[String]): Unit = {
//    创建一个本地StreamingContext，其中两个工作线程和批处理间隔为1秒
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

//    创建一个连接到localhost:port的DStream
//    val lines = ssc.socketTextStream("localhost", 9999)

//    val words = lines.flatMap(_.split(" "))
//    每个批次word计数
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)

//    wordCounts.print()

    /**
      * 输入Dstream
      * 基本来源：文件系统、端口连接
      * 高级来源：kafka、flume、kinesis .etc，需要第三方依赖
      */

//    输入源为文件的DStream
//    val ds = ssc.fileStream("/opt/modules/hadoop-3.0.3/logs")
//    ds.print()

    /**
      * 基于Direct方式消费kafka数据源
      * 开启checkpoint，设置恰好一次(Exactly-once-semantics)
      * 来保证数据消费唯一性
      */

    ssc.checkpoint("/opt/development/checkpoint")
    val topic = Set("user_pro")
    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> "lee:9092",
      "key.serializer" -> "value.serializer",
      "refresh.leader.backoff.ms" -> "5000",
      "group.id" -> "baymax_SparkStraming"
    )

    val ds1 = KafkaUtils.createDirectStream[String, String,StringDecoder,StringDecoder](ssc, kafkaParams, topic).map(_._2)

    val ds2 = ds1.map(f=>(JSON.parseObject(f)
      .getInteger("star_sign").toInt,1))

    val ds3 = ds1.filter(f=>JSON.parseObject(f).getInteger("age")<=20)

    val ds4 = ds3.repartition(4)

    val ds5 = ds3.union(ds1)

//    reduce聚合，筛选batch年龄最小的
    val ds6 = ds1.reduce((x,y)=>{
      if(JSON.parseObject(x).getInteger("age")>JSON
        .parseObject(y).getIntValue("age"))
        y
      else x
    })

//    每隔10秒计算前15秒年龄最小的
    val ds7 = ds1.reduceByWindow((x,y)=>{
      if(JSON.parseObject(x).getInteger("age")>JSON
        .parseObject(y).getIntValue("age"))
        y
      else x
    },Seconds(15),Seconds(10))

//    DStream[K]=>DStream[K,Long]
    implicit val cmo = new Ordering[String]{
      override def compare(x:String,y:String):Int =
        -JSON.parseObject(x).getInteger("age").compareTo(JSON.parseObject(y).getInteger("age"))
    }
    val ds8 = ds1.countByValue(2)(cmo)

    val ds9 = ds2.reduceByKey(_+_)
//    每隔10秒计算各星座出现次数
    val ds10 = ds2.reduceByKeyAndWindow(_+_, Seconds(10))

    val ds11 = ds2.join(ds10)

    val ds12 = ds2.cogroup(ds10)

    /**
      * updateStateByKey和mapWithState
      * online计算：batch=>window=>全局所有时间间隔内产生的数据
      * updateStateByKey:统计全局key的状态，就算没有数据输入，也会在每个批次返回key状态
      * mapWithState:统计全局key状态，但如果没有数据输入，便不会返回之前的key的状态
      * 区别:updateStateByKey数据量太大，需要checkpoint，会占用较大内存
      * 对于没有输入，不会返回那些没有变化的key数据，checkpoint
      * 不会像updateStateByKey，占用太多内存
      */
    val ds13 = ds2.updateStateByKey(
      (value:Seq[Int], state:Option[Int])=>{
      val values = value.sum
      Some(values + state.getOrElse(0))
    })

    val initialRDD = ssc.sparkContext.parallelize(List[(Int,Int)]())
    val mappingFunction=
      (key:Int,value:Option[Int], state:State[Int]) => {
      val newState = state.getOption().getOrElse(0) + value.getOrElse(0)
      state.update(newState)
      (key, newState)
    }
    val ds14 = ds2.mapWithState(StateSpec.function(mappingFunction).initialState(initialRDD))

    val ds15 = ds2.window(Seconds(10))

    val ds16 = ds2.countByWindow(Seconds(15),Seconds(10))

    val ds17 = ds2.countByValueAndWindow(Seconds(15), Seconds(10))

    val ds18 = ds2.join(ds15)

//    ds18.print()

    /**
      * saveAsTextFiles传参为prefix和suffix
      * 路径保存在prefix
      * 若hdfs，则设置为hdfs://lee:8020/
      */
//    ds1.saveAsTextFiles("hdfs://lee:8020/logs/user_pro_logs", "log")
//    ds1.saveAsObjectFiles("/opt/data/logs/user_pro_logs", "object")

    val tableName = "user_pro"
    val cf = "info"
    val columns = Array("uid","ceil","qq","age",
      "birthday","height","is_married",
      "duration_of_menstruation","menstrual_cycle",
      "star_sign","weight","province","city","recipient","recip_ceil")

    ds1.foreachRDD(rdd=>{
      rdd.foreachPartition(records=>{
        HBaseUtils1x.init()
        val puts = new ArrayBuffer[Put]()
        records.foreach(f=>{
          val uid = JSON.parseObject(f).getString("uid")
          val ceil = JSON.parseObject(f).getString("ceil")
          val qq = JSON.parseObject(f).getString("qq")
          val age = JSON.parseObject(f).getString("age")
          val birthday = JSON.parseObject(f).getString("birthday")
          val height = JSON.parseObject(f).getString("height")
          val is_married = JSON.parseObject(f).getString("is_married")
          val duration_of_menstruation = JSON.parseObject(f).getString("duration_of_menstruation")
          val menstrual_cycle = JSON.parseObject(f).getString("menstrual_cycle")
          val star_sign = JSON.parseObject(f).getString("star_sign")
          val weight = JSON.parseObject(f).getString("weight")
          val province = JSON.parseObject(f).getString("province")
          val city = JSON.parseObject(f).getString("city")
          val recipient = JSON.parseObject(f).getString("recipient")
          val recip_ceil = JSON.parseObject(f).getString("recip_ceil")

          /**
            * 服务端建表
            * create 'user_pro',
            * {NAME=>'info',BLOCKSIZE=>'16384',BLOCKCACHE=>'false',TTL=>86400},
            * {NUMREGIONS=>10,SPLITALGO=>'HexStringSplit'}
            * rowkey设计：
            * 哈希散列化，取前8位+本身
            */
          val rowKey = MD5Hash.getMD5AsHex(Bytes.toBytes
          (uid)).substring(0,8) + "_" + uid
          val cols = Array(uid,ceil,qq,age,birthday,height,is_married,duration_of_menstruation,menstrual_cycle,star_sign,weight,province,city,recipient,recip_ceil)
          try{
            puts.append(HBaseUtils1x.getPutAction(rowKey,
              cf, columns, cols))
          }catch{
            case e:Throwable => println(f)
          }
        })
        import collection.JavaConverters._
        HBaseUtils1x.addDataBatchEx(tableName, puts.asJava)
        HBaseUtils1x.closeConnection()
      })
    })
    var rddQueue = new mutable.SynchronizedQueue[RDD[Int]]
    val queueStream = ssc.queueStream(rddQueue)
    val result = queueStream.map(x=>(x%5,1)).reduceByKey(_+_)
    result.print()
    ssc.start()
    while(true){
      rddQueue += ssc.sparkContext.makeRDD(1 to 100,2)
    }
    ssc.awaitTermination()
  }
}
