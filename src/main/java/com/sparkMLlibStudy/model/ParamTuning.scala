package com.sparkMLlibStudy.model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-9-13 下午3:11
  * @Modified By:
  */
object ParamTuning {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ClusterModel")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

//    训练数据格式(id, text, label)
    val train = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0),
      (4L, "b spark who", 1.0),
      (5L, "g d a y", 0.0),
      (6L, "spark fly", 1.0),
      (7L, "was mapreduce", 0.0),
      (8L, "e spark program", 1.0),
      (9L, "a e c l", 0.0),
      (10L, "spark compile", 1.0),
      (11L, "hadoop software", 0.0)
    )).toDF("id", "text", "label")

//    配置ml管道, 由tokenizer, hashingTF和lr组成
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

//    使用ParamGridBuilder构建网格参数寻优
//    包括hashingTF三个参数和lr两个参数，共计6个参数
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    /**
      * 将Pipeline视为Estimator，将其包装在CrossValidator实例中
      * 允许我们共同选择所有Pipeline阶段的参数
      * CrossValidator需要Estimator，一组Estimator ParamMaps和一个Evaluator
      * 此处的求值程序是BinaryClassificationEvaluator及其默认度量标准是areaUnderROC
      */
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)
      .setParallelism(2)

//    运行交叉验证，并选择最佳参数集
    val cvModel = cv.fit(train)

//    测试集
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

//    模型预测
    cvModel.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: org.apache.spark.ml.linalg.Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

//    训练集和测试集
    val data = spark.read.format("libsvm").load("/opt/modules/spark-2.3.1/data/mllib/sample_linear_regression_data.txt")
    val Array(training, testing) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lnr = new LinearRegression()
      .setMaxIter(10)

//    ParamGridBuilder构建网格参数寻优
//    TrainValidationSplit遍历所有组合并搜索最有参数
    val paramGridLnr = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

//    TrainValidationSplit参数寻优与评估
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lnr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGridLnr)
//      80%数据作训练集
      .setTrainRatio(0.8)
      .setParallelism(2)

//    寻找最佳参数
    val lnrModel = trainValidationSplit.fit(training)

//    模型评估
    lnrModel.transform(testing)
      .select("features", "label", "prediction")
      .show(false)
  }
}
