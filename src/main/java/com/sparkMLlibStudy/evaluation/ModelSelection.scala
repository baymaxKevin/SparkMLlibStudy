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

    val spark = SparkSession.builder.appName("ctrModel").getOrCreate()

    val resourcesPath = this.getClass.getResource("/samples.snappy.orc")
    val rawSamples = spark.read.format("orc").option("compression", "snappy").load(resourcesPath.getPath)
    rawSamples.printSchema()
    rawSamples.show(10)

    //将数组转换为向量，以便后续的vectorAssembler使用
    val samples = new FeatureEngineering().transferArray2Vector(rawSamples)

    //将样本分为训练样本和验证样本
    val Array(trainingSamples, validationSamples) = samples.randomSplit(Array(0.7, 0.3))
    val evaluator = new Evaluator



    println("Logistic Regression Ctr Prediction Model:")
    val lrModel = new LogisticRegressionCtrModel()
    lrModel.train(trainingSamples)
    evaluator.evaluate(lrModel.transform(validationSamples))

    println("FM Ctr Prediction Model:")
    val fmModel = new FactorizationMachineCtrModel()
    fmModel.train(trainingSamples)
    evaluator.evaluate(fmModel.transform(validationSamples))

    println("Neural Network Ctr Prediction Model:")
    val nnModel = new NeuralNetworkCtrModel()
    nnModel.train(trainingSamples)
    evaluator.evaluate(nnModel.transform(validationSamples))

    println("Random Forest Ctr Prediction Model:")
    val rfModel = new RandomForestCtrModel()
    rfModel.train(trainingSamples)
    evaluator.evaluate(rfModel.transform(validationSamples))

    println("GBDT Ctr Prediction Model:")
    val gbtModel = new GBDTCtrModel()
    gbtModel.train(trainingSamples)
    evaluator.evaluate(gbtModel.transform(validationSamples))

    println("GBDT+LR Ctr Prediction Model:")
    val gbtlrModel = new GBTLRCtrModel()
    gbtlrModel.train(trainingSamples)
    evaluator.evaluate(gbtlrModel.transform(validationSamples))


    println("IPNN Ctr Prediction Model:")
    val ipnnModel = new InnerProductNNCtrModel()
    ipnnModel.train(trainingSamples)
    evaluator.evaluate(ipnnModel.transform(validationSamples))


    println("OPNN Ctr Prediction Model:")
    val opnnModel = new OuterProductNNCtrModel()
    opnnModel.train(trainingSamples)
    evaluator.evaluate(opnnModel.transform(validationSamples))
  }

}
