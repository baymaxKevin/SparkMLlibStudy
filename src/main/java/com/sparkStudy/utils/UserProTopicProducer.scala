package com.sparkStudy.utils

import java.util
import java.util.{Properties, UUID}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{ArraySerializer, SerializeFilter, SerializerFeature}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.sql.SparkSession

/**
  * 读取mysql表user_pro的数据，并创建kafka生产者
  */
object UserProTopicProducer {
  def main(args: Array[String]): Unit = {
//    topic名称：user_pro
    val topic = "user_pro"
//    节点ip和port
    val brokers = "lee:9092"
    val props = new Properties()
//    设置kakfa属性
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class","kafka.serializer" +
      ".StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String,String](kafkaConfig)

    val spark = SparkSession.builder()
      .appName("UserProTopicProducer")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read
      .format("jdbc")
      .option("url",
        "jdbc:mysql://lee:3306/meiyou?createDatabaseIfNotExist=true&useSSL=false")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("dbtable","user_pro")
      .option("user","root")
      .option("password","EOSspark123")
      .load()

    import spark.implicits._
    val rdd1 = df.as[UserPro]
      .rdd.map(f=>{
      val maps = new util.HashMap[String,String]()
      val keys = Array("uid","ceil","qq","age",
        "birthday","height","is_married",
        "duration_of_menstruation","menstrual_cycle",
        "star_sign", "weight","province","city",
        "recipient","recip_ceil")
      val values = Array(f.uid.toString,f.ceil,f.qq,f.age
        .toString,f.birthday,f.height.toString,f
        .is_married.toString,f.duration_of_menstruation.toString,
        f.menstrual_cycle.toString,f.star_sign.toString,f
          .weight.toString, f.province,f.city,f.recipient,f.recip_ceil)
      keys.zip(values).foreach(f=>maps.put(f._1,f._2))
      JSON.toJSON(maps).toString
    })
    val rdd2 = df.as[UserPro].rdd.map(f=>JSON
      .toJSONString(f,SerializerFeature.PrettyFormat))
    rdd1.take(5).foreach(println(_))
    rdd2.take(5).foreach(println(_))
  }
  case class UserPro(var uid:Int,var ceil:String,var qq:String,
                     var age:Int,var birthday:String,
                     var height:Double,var is_married:Int,
                     var duration_of_menstruation:Int,
                     var menstrual_cycle:Int,var star_sign:Int,
                     var weight:Double,var province:String,city:String,
                     var recipient:String,var recip_ceil:String)
}
