package com.sparkMLlibStudy.evaluation

import com.sparkMLlibStudy.ctrModel._
import com.sparkMLlibStudy.features.FeatureEngineering
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-12-4 下午6:27
  * @Modified By:
  */
object ModelSelection {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("ctrModel")
      .master("local[2]")
      .getOrCreate()

    val rawSamples = spark.read.format("orc").option("compression", "snappy").load("/opt/data/samples.snappy.orc")
    rawSamples.printSchema()
    println("----------------加载数据----------------")
    rawSamples.show(10,false)

    //将数组转换为向量，以便后续的vectorAssembler使用
    val samples = new FeatureEngineering().transferArray2Vector(rawSamples)
    println("----------------embedding数组转向量----------------")
    samples.show(10, false)

    //将样本分为训练样本和验证样本
    val Array(trainingSamples, validationSamples) = samples.randomSplit(Array(0.7, 0.3))
    val evaluator = new Evaluator

    println("逻辑回归CTR预估模型:")
    val lrModel = new LogisticRegressionCtrModel()
    val time1 = System.currentTimeMillis()
    lrModel.train(trainingSamples)
    val time2 = System.currentTimeMillis()
    evaluator.evaluate(lrModel.transform(validationSamples))
    val time3 = System.currentTimeMillis()
    println(s"LR训练耗时:${time2-time1}ms, LR预测评估耗时:${time3-time2}ms")

    println("FM Ctr预估模型:")
    val fmModel = new FactorizationMachineCtrModel()
    val time4 = System.currentTimeMillis()
    fmModel.train(trainingSamples)
    val time5 = System.currentTimeMillis()
    evaluator.evaluate(fmModel.transform(validationSamples))
    val time6 = System.currentTimeMillis()
    println(s"FM训练耗时:${time5-time4}ms, LR预测评估耗时:${time6-time5}ms")

    println("神经网络CTR预估模型:")
    val nnModel = new NeuralNetworkCtrModel()
    val time7 = System.currentTimeMillis()
    nnModel.train(trainingSamples)
    val time8 = System.currentTimeMillis()
    evaluator.evaluate(nnModel.transform(validationSamples))
    val time9 = System.currentTimeMillis()
    println(s"FM训练耗时:${time8-time7}ms, LR预测评估耗时:${time9-time8}ms")

    println("随机森林CTR预估模型:")
    val rfModel = new RandomForestCtrModel()
    val time10 = System.currentTimeMillis()
    rfModel.train(trainingSamples)
    val time11 = System.currentTimeMillis()
    evaluator.evaluate(rfModel.transform(validationSamples))
    val time12 = System.currentTimeMillis()
    println(s"随机森林训练耗时:${time11-time10}ms,随机森林预测评估耗时:${time12-time11}ms")

    println("GBDT CTR预估模型:")
    val gbtModel = new GBDTCtrModel()
    val time13 = System.currentTimeMillis()
    gbtModel.train(trainingSamples)
    val time14 = System.currentTimeMillis()
    evaluator.evaluate(gbtModel.transform(validationSamples))
    val time15 = System.currentTimeMillis()
    println(s"GBDT训练耗时:${time14-time13}ms, LR预测评估耗时:${time15-time14}ms")

    println("GBDT+LR CTR预估模型:")
    val gbtlrModel = new GBTLRCtrModel()
    val time16 = System.currentTimeMillis()
    gbtlrModel.train(trainingSamples)
    val time17 = System.currentTimeMillis()
    evaluator.evaluate(gbtlrModel.transform(validationSamples))
    val time18 = System.currentTimeMillis()
    println(s"GBDT+LR训练耗时:${time17-time16}ms, LR预测评估耗时:${time18-time17}ms")

    println("IPNN CTR预估模型:")
    val ipnnModel = new InnerProductNNCtrModel()
    val time19 = System.currentTimeMillis()
    ipnnModel.train(trainingSamples)
    val time20 = System.currentTimeMillis()
    evaluator.evaluate(ipnnModel.transform(validationSamples))
    val time21 = System.currentTimeMillis()
    println(s"IPNN训练耗时:${time20-time19}ms, LR预测评估耗时:${time21-time20}ms")


    println("OPNN CTR预估模型:")
    val opnnModel = new OuterProductNNCtrModel()
    val time22 = System.currentTimeMillis()
    opnnModel.train(trainingSamples)
    val time23 = System.currentTimeMillis()
    evaluator.evaluate(opnnModel.transform(validationSamples))
    val time24 = System.currentTimeMillis()
    println(s"OPNN训练耗时:${time23-time22}ms, LR预测评估耗时:${time24-time23}ms")
  }

}
