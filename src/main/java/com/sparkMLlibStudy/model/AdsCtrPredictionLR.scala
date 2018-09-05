package com.sparkMLlibStudy.model

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{FeatureHasher, StringIndexer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-9-4 下午1:53
  * @Modified By:
  */
object AdsCtrPredictionLR {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AdsCtrPredictionLR")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    /**
      * id和click分别为广告的id和是否点击广告
      * site_id,site_domain,site_category,app_id,app_domain,app_category,device_id,device_ip,device_model为分类特征，需要OneHot编码
      * device_type,device_conn_type,C14,C15,C16,C17,C18,C19,C20,C21为数值特征，直接使用
      */
    val data = spark.read.csv("/opt/data/ads_6M.csv").toDF(
      "id","click","hour","C1","banner_pos","site_id","site_domain",
      "site_category","app_id","app_domain","app_category","device_id","device_ip",
      "device_model","device_type","device_conn_type","C14","C15","C16","C17","C18",
      "C19","C20","C21")
    data.show(5,false)

    val splited = data.randomSplit(Array(0.7,0.3),2L)
    val catalog_features = Array("click","site_id","site_domain","site_category","app_id","app_domain","app_category","device_id","device_ip","device_model")
    var train_index = splited(0)
    var test_index = splited(1)
    for(catalog_feature <- catalog_features){
      val indexer = new StringIndexer()
        .setInputCol(catalog_feature)
        .setOutputCol(catalog_feature.concat("_index"))
      val train_index_model = indexer.fit(train_index)
      val train_indexed = train_index_model.transform(train_index)
      val test_indexed = indexer.fit(test_index).transform(test_index,train_index_model.extractParamMap())
      train_index = train_indexed
      test_index = test_indexed
    }
    println("字符串编码下标标签：")
    train_index.show(5,false)
    test_index.show(5,false)

//    特征Hasher
    val hasher = new FeatureHasher()
      .setInputCols("site_id_index","site_domain_index","site_category_index","app_id_index","app_domain_index","app_category_index","device_id_index","device_ip_index","device_model_index","device_type","device_conn_type","C14","C15","C16","C17","C18","C19","C20","C21")
      .setOutputCol("feature")

    println("特征Hasher编码：")
    val train_hs = hasher.transform(train_index)
    val test_hs = hasher.transform(test_index)

    /**
      * LR建模
      * setMaxIter设置最大迭代次数(默认100),具体迭代次数可能在不足最大迭代次数停止(见下一条)
      * setTol设置容错(默认1e-6),每次迭代会计算一个误差,误差值随着迭代次数增加而减小,当误差小于设置容错,则停止迭代
      * setRegParam设置正则化项系数(默认0),正则化主要用于防止过拟合现象,如果数据集较小,特征维数又多,易出现过拟合,考虑增大正则化系数
      * setElasticNetParam正则化范式比(默认0),正则化有两种方式:L1(Lasso)和L2(Ridge),L1用于特征的稀疏化,L2用于防止过拟合
      * setLabelCol设置标签列
      * setFeaturesCol设置特征列
      * setPredictionCol设置预测列
      * setThreshold设置二分类阈值
      */
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0)
      .setFeaturesCol("feature")
      .setLabelCol("click_index")
      .setPredictionCol("click_predict")

    val model_lr = lr.fit(train_hs)

    println(s"每个特征对应系数: ${model_lr.coefficients} 截距: ${model_lr.intercept}")

    val predictions = model_lr.transform(test_hs)
    predictions.select("click_index","click_predict","probability").show(100,false)

    val predictionRdd = predictions.select("click_predict","click_index").rdd.map{
      case Row(click_predict:Double,click_index:Double)=>(click_predict,click_index)
    }
    val metrics = new MulticlassMetrics(predictionRdd)

    val accuracy = metrics.accuracy
    val weightedPrecision = metrics.weightedPrecision
    val weightedRecall = metrics.weightedRecall
    val f1 = metrics.weightedFMeasure

    println(s"LR评估结果：\n分类正确率：${accuracy}\n加权正确率：${weightedPrecision}\n加权召回率：${weightedRecall}\nF1值：${f1}")


  }
}
