package com.sparkStudy.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-10-23 上午11:53
  * @Modified By:
  */
object ReadEsDB {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("es.nodes","localhost")
      .set("es.prot","9200")
      .setMaster("local[2]")
      .setAppName("ReadEsDB")

    val sc = new SparkContext(conf)

    val relation = Map("LiYanHong"->"BaiDu","DingLei"->"WangYi")
    val zone = Map("BeiJing"->"China","GuangZhou"->"GuangDong")

    val rdd = sc.makeRDD(Seq(relation,zone))
//    EsSpark.saveToEs(rdd,"relation/zone")
//    println("写入ES成功！")
    val rddFromEs = EsSpark.esRDD(sc,"relation/zone","?q=Li*")
    println("读入ES成功！")
    println(rddFromEs.count())
  }
}
