package com.sparkMLlibStudy.model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression._
import org.apache.spark.sql.SparkSession

/**
  * @Author: JZ.lee
  * @Description: 回归
  *              1.线性回归(linear regression)
  * @Date: 18-9-7 上午11:45
  * @Modified By:
  */
object RegressionModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RegressionModel")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

//    加载训练数据
    val training = spark.read.format("libsvm")
      .load("/opt/modules/spark-2.3.1/data/mllib/sample_linear_regression_data.txt")

    /**
      * 线性回归
      */
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

//    训练线性模型
    val lrModel = lr.fit(training)

//    打印线性模型系数
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

//    评估线性模型
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    /**
      * Generalized linear regression(广义线性回归)
      * 类型：Gaussian、Binomial、Poisson、Gamma、Tweedie
      */
    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .setLink("identity")
      .setMaxIter(10)
      .setRegParam(0.3)

    val glrModel = glr.fit(training)

    println(s"Coefficients: ${glrModel.coefficients}")
    println(s"Intercept: ${glrModel.intercept}")

    val summary = glrModel.summary
    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    summary.residuals().show()

//    加载libsvm格式数据
    val data = spark.read.format("libsvm").load("/opt/modules/spark-2.3.1/data/mllib/sample_libsvm_data.txt")

//    自动检测分类特征(distinct count < 4)并转化为下标
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

//    数据集分割为训练集和测试集，比例为0.7:0.3
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    /**
      * Decision tree regression(决策树回归)
      */
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

//    索引器和回归树添加pipeline中
    val pipeline1 = new Pipeline()
      .setStages(Array(featureIndexer, dt))

    val dtModel = pipeline1.fit(trainingData)

//    预测
    val predictionDt = dtModel.transform(testData)

    predictionDt.select("prediction", "label", "features").show(5,false)

//    评价模型
    val evalDt = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmseDt = evalDt.evaluate(predictionDt)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmseDt")

    val tree_model = dtModel.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    println(s"Learned regression tree model:\n ${tree_model.toDebugString}")

    /**
      * Random forest regression(随机森林回归)
      */
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    val pipeline2 = new Pipeline()
      .setStages(Array(featureIndexer, rf))

    val rfModel = pipeline2.fit(trainingData)

    val predictionRf = rfModel.transform(testData)

    predictionRf.select("prediction", "label", "features").show(5)

    val evalRf = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmseRf = evalRf.evaluate(predictionRf)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmseRf")

    val rf_model = rfModel.stages(1).asInstanceOf[RandomForestRegressionModel]
    println(s"Learned regression forest model:\n ${rf_model.toDebugString}")

    /**
      * Gradient-boosted tree regression
      */
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)

//    索引器和GBRT组装到管道中
    val pipeline3 = new Pipeline()
      .setStages(Array(featureIndexer, gbt))

    val gbdtModel = pipeline3.fit(trainingData)

    val predictionGbdt= gbdtModel.transform(testData)

    predictionGbdt.select("prediction", "label", "features").show(5,false)

    val evalGbdt = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmseGbdt = evalGbdt.evaluate(predictionGbdt)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmseGbdt")

    val gbt_model = gbdtModel.stages(1).asInstanceOf[GBTRegressionModel]
    println(s"Learned regression GBT model:\n ${gbt_model.toDebugString}")


  }
}
