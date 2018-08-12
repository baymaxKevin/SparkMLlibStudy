package com.sparkStudy.utils

import java.util.Properties

import com.google.gson.Gson
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
    val user_pro = df.as[UserPro].rdd.map(f=>new Gson().toJson(f)).collect()

    while(true){
      for(upo <- user_pro) {
        println(upo)
        val message = new KeyedMessage[String,String](topic, upo)
        producer.send(message)
        Thread.sleep(1000)
      }
    }
  }
  case class UserPro(var uid:Int,var ceil:String,var qq:String,
                     var age:Int,var birthday:String,
                     var height:Double,var is_married:Int,
                     var duration_of_menstruation:Int,
                     var menstrual_cycle:Int,var star_sign:Int,
                     var weight:Double,var province:String,city:String,
                     var recipient:String,var recip_ceil:String)
}
