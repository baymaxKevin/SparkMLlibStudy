package com.sparkMLlibStudy.model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.{SparkSession, functions}

/**
  * 分类
  * 1. 逻辑回归(Logistic regression)
  * 2. 决策树分类(Decision tree classifier)
  * 3. 随机森林分类(Random forest classifier)
  * 4. 梯度提升树分类(Gradient-boosted tree classifier)
  * 5. 多层感知分类器(Multilayer perceptron classifier)
  * 6. 线性支持向量机(Linear Support Vector Machine)
  * 7. One-vs-Rest classifier (a.k.a. One-vs-All)
  * 8. 朴素贝叶斯(Naive Bayes)
  * 注意：spark ml基于DataFrame，而mllib基于rdd
  * 官方推荐ml，ml在dataframe的抽象级别更高，数据和操作耦合度更低
  */
object ClassificationModel {
  def main(args: Array[String]): Unit = {
    /**
      * 逻辑回归是预测分类响应的常用方法
      * 广义线性模型，建立代价函数，通过优化方法迭代求求解出最优模型参数
      * 在spark.ml中，逻辑回归可以用于通过二项逻辑回归来预测二
      * 分类结果，或者它可以用于通过使用多项逻辑回归来预测多类结果；
      * 使用family参数在这两个算法之间进行选择，或者保持不设置，Spark将推断出正确的变量
      */
      val spark = SparkSession.builder()
        .appName("ClassificationModel")
        .master("local[2]")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
//    加载训练数据
    val training = spark.read
      .format("libsvm")
      .load("/opt/modules/spark-2.3.1/data/mllib/sample_libsvm_data.txt")

    training.show(5,false)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

//    模型拟合
    val lrModel = lr.fit(training)

//    打印逻辑回归的系数和截距
//    println(s"Coefficients: ${lrModel.coefficients}")
//    println(s"Intercept: ${lrModel.intercept} ")

    val trainingSummary = lrModel.binarySummary

//    获取每次迭代目标,每次迭代的损失,会逐渐减少
    val objectiveHistory = trainingSummary.objectiveHistory
//    println("objectiveHistory:")
//    objectiveHistory.foreach(loss => println(loss))

//    获取接收器操作特性作为dataframe和roc
    val roc = trainingSummary.roc
//    roc.show()
//    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

//    设置模型阈值以最大化F-Measure
    val fMeasure = trainingSummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(functions.max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where(fMeasure("F-Measure") === maxFMeasure)
      .select("threshold").head().getDouble(0)
    lrModel.setThreshold(bestThreshold)
//    println(s"Threshold: ${lrModel.getThreshold}")

    //    还可以使用多项式族进行二进制分类
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

//    打印逻辑回归的系数和截距
//    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
//    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    /**
      * Multinomial logistic regression(多分类)
      * 将逻辑回归一般化成多类别问题得到的分类方法
      * 在多项Logistic回归中，算法产生维数K×J的矩阵，其中K是结果类的数量，J是特征的数量
      * 美团点评:dataunion.org/17006.html
      */
    val train1 = spark
      .read
      .format("libsvm")
      .load("/opt/modules/spark-2.3.1/data/mllib/sample_multiclass_classification_data.txt")

    val lrModel1 = lr.fit(train1)
//    println(s"Coefficients: \n${lrModel1.coefficientMatrix}")
//    println(s"Intercepts: \n${lrModel1.interceptVector}")
    val trainingSummary1 = lrModel1.summary
    val objectiveHistory1 = trainingSummary1.objectiveHistory
//    println("objectiveHistory:")
//    objectiveHistory1.foreach(println)
//
//    println("False positive rate by label:")
//    trainingSummary1.falsePositiveRateByLabel
//      .zipWithIndex.foreach { case (rate, label) =>
//      println(s"label $label: $rate")
//    }
//
//    println("True positive rate by label:")
//    trainingSummary1.truePositiveRateByLabel.zipWithIndex
//      .foreach { case (rate, label) =>
//      println(s"label $label: $rate")
//    }
//
//    println("Precision by label:")
//    trainingSummary1.precisionByLabel.zipWithIndex
//      .foreach { case (prec, label) =>
//      println(s"label $label: $prec")
//    }
//
//    println("Recall by label:")
//    trainingSummary1.recallByLabel.zipWithIndex.foreach {
//      case (rec, label) =>
//      println(s"label $label: $rec")
//    }
//
//
//    println("F-measure by label:")
//    trainingSummary1.fMeasureByLabel.zipWithIndex.foreach
//    { case (f, label) =>
//      println(s"label $label: $f")
//    }

    val accuracy = trainingSummary.accuracy
    val falsePositiveRate =
      trainingSummary1.weightedFalsePositiveRate
    val truePositiveRate =
      trainingSummary1.weightedTruePositiveRate
    val fMeasure1 = trainingSummary1.weightedFMeasure
    val precision = trainingSummary1.weightedPrecision
    val recall = trainingSummary1.weightedRecall
//    println(s"Accuracy: $accuracy\nFPR: $falsePositiveRate\nTPR: $truePositiveRate\n" +
//      s"F-measure: $fMeasure1\nPrecision: $precision \nRecall: $recall")

    /**
      * Decision tree classifier
      */

//    索引标签，将元数据添加到标签列;
//    适合整个数据集以包含索引中的所有标签
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(training)
//    自动识别分类特征并对其进行索引
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // 具有> 4个不同值的特征被视为连续的
      .fit(training)

//    数据集分为训练集和测试集 (30%作为测试集)
    val Array(trainingData, testData) = training.randomSplit(Array(0.7, 0.3))

//    训练决策树模型
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
//      .setImpurity("entropy") // 不纯度
//      .setMaxBins(100) // 离散化"连续特征"的最大划分数
//      .setMaxDepth(5) // 树的最大深度
//      .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
//      .setMinInstancesPerNode(10) //每个节点包含的最小样本数
//      .setSeed(123456)

//    将索引标签转换为原始标签
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

//    链索引器和管道中的树
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

//    运行索引器模型训练
    val model = pipeline.fit(trainingData)

//    预测
    val predictions = model.transform(testData)

//    predictions.select("predictedLabel", "label", "features").show(5,false)

//    选择（预测标签，实际标签），并计算测试误差
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val acc = evaluator.evaluate(predictions)
//    println(s"Test Error = ${(1.0 - acc)}")

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
//    println(s"Learned classification tree model:\n ${treeModel.toDebugString}")

//    训练随机森林模型
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

//    索引标签转换为原始标签
    val labelConverter1 = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    val pipeline1 = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter1))

    val modelRF = pipeline1.fit(trainingData)

    val preds = modelRF.transform(testData)

    preds.select("predictedLabel","label","features").show(5,false)

    val eval1 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accy = eval1.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accy)}")

    val rfModel = modelRF.stages(2).asInstanceOf[RandomForestClassificationModel]
    println(s"Learned classification forest model:\n ${rfModel.toDebugString}")
  }
}
