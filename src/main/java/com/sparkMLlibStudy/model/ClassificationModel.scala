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
      * 在spark.ml中，逻辑回归可以用于通过二项逻辑回归来预测二分类结果
      * 或者它可以用于通过使用多项逻辑回归来预测多类结果
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

    println("训练集数据结构如下:")
    training.show(5,false)
    training.describe()

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

//    模型拟合
    val lrModel = lr.fit(training)

//    打印逻辑回归的系数和截距
    println(s"Coefficients: ${lrModel.coefficients}")
    println(s"Intercept: ${lrModel.intercept} ")

    val trainingSummary = lrModel.binarySummary

//    获取每次迭代目标,每次迭代的损失,会逐渐减少
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

//    获取roc
    val roc = trainingSummary.roc
    roc.show(false)
    println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

//    设置模型阈值为最大化F-Measure
    val fMeasure = trainingSummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(functions.max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where(fMeasure("F-Measure") === maxFMeasure)
      .select("threshold").head().getDouble(0)
    lrModel.setThreshold(bestThreshold)
    println(s"Threshold: ${lrModel.getThreshold}")

    val accuracyLr = lrModel.evaluate(training).accuracy
    val weightedPrecisionLr = lrModel.evaluate(training).weightedPrecision
    val weightedRecallLr = lrModel.evaluate(training).weightedRecall
    println(s"accuray: ${accuracyLr}\nweightedPrecision:${weightedPrecisionLr}\nweightedRecallLr:${weightedRecallLr}")

    //    还可以使用多项式族进行二进制分类
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(training)

//    打印逻辑回归的系数和截距
    println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
    println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

    val trainingSummaryMlr = mlrModel.binarySummary

//    获取每次迭代loss
    val objectiveHistoryMlr = trainingSummaryMlr.objectiveHistory
    println("objectiveHistory:")
    objectiveHistoryMlr.foreach(loss => println(loss))
//    获取roc和areaUnderROC
    val rocMlr = trainingSummary.roc
    rocMlr.show(false)
    println(s"areaUnderROC: ${trainingSummaryMlr.areaUnderROC}")
//    设置模型阈值为最大F1
    val fMeasureMlr = trainingSummaryMlr.fMeasureByThreshold
    val maxFMeasureMlr = fMeasureMlr.select(functions.max("F-Measure")).head().getDouble(0)
    val bestThresholdMlr = fMeasureMlr.where(fMeasureMlr("F-Measure") === maxFMeasureMlr)
      .select("threshold").head().getDouble(0)
    mlr.setThreshold(bestThresholdMlr)

    val accuracyMlr = mlrModel.evaluate(training).accuracy
    val weightedPrecisionMlr = mlrModel.evaluate(training).weightedPrecision
    val weightedRecallMlr = mlrModel.evaluate(training).weightedRecall
    println(s"accuray: ${accuracyMlr}\nweightedPrecision:${weightedPrecisionMlr}\nweightedRecallLr:${weightedRecallMlr}")
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
    println(s"Coefficients: \n${lrModel1.coefficientMatrix}")
    println(s"Intercepts: \n${lrModel1.interceptVector}")
    val trainingSummary1 = lrModel1.summary
    val objectiveHistory1 = trainingSummary1.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory1.foreach(println)

//    根据每个标签查询指标
    println("False positive rate by label:")
    trainingSummary1.falsePositiveRateByLabel
      .zipWithIndex.foreach { case (rate, label) =>
      println(s"label $label: $rate")
    }

    println("True positive rate by label:")
    trainingSummary1.truePositiveRateByLabel.zipWithIndex
      .foreach { case (rate, label) =>
      println(s"label $label: $rate")
    }

    println("Precision by label:")
    trainingSummary1.precisionByLabel.zipWithIndex
      .foreach { case (prec, label) =>
      println(s"label $label: $prec")
    }

    println("Recall by label:")
    trainingSummary1.recallByLabel.zipWithIndex.foreach {
      case (rec, label) =>
      println(s"label $label: $rec")
    }


    println("F-measure by label:")
    trainingSummary1.fMeasureByLabel.zipWithIndex.foreach
    { case (f, label) =>
      println(s"label $label: $f")
    }

    val accuracy1 = trainingSummary1.accuracy
    val falsePositiveRate =
      trainingSummary1.weightedFalsePositiveRate
    val truePositiveRate =
      trainingSummary1.weightedTruePositiveRate
    val fMeasure1 = trainingSummary1.weightedFMeasure
    val precision1 = trainingSummary1.weightedPrecision
    val recall1 = trainingSummary1.weightedRecall
    println(s"Accuracy: $accuracy1\nFPR: $falsePositiveRate\nTPR: $truePositiveRate\n" +
      s"F-measure: $fMeasure1\nPrecision: $precision1 \nRecall: $recall1")

    /**
      * Decision tree classifier(决策树)
      * 树形模型是一个一个特征进行处理，而线性模型是所有特征给予权重相加得到一个新的值
      * 逻辑回归是将所有特征变换为概率后，通过大于某一概率阈值的划分为一类，小于某一概率阈值的为另一类；而决策树是对每一个特征做一个划分
      * 决策树思想，实际上就是寻找最纯净的划分方法，纯净(纯度)衍生如下：ID3->信息增益,C4.5->信息增益率,CART->基尼系数
      * 建树和裁剪
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
      .setImpurity("entropy") // 不纯度
      .setMaxBins(100) // 离散化"连续特征"的最大划分数
      .setMaxDepth(5) // 树的最大深度
      .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
      .setMinInstancesPerNode(10) //每个节点包含的最小样本数
      .setSeed(123456L)

//    将索引标签转换为原始标签
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

//    索引器和决策树组装到管道中
    val pipelineDt = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

//    训练管道中模型
    val dtModel = pipelineDt.fit(trainingData)

//    预测
    val predictionDt = dtModel.transform(testData)

    predictionDt.select("predictedLabel", "label", "features").show(5,false)

//    选择（预测标签，实际标签），并计算测试误差
    val evalDt = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val acc = evalDt.evaluate(predictionDt)
    println(s"Test Error = ${(1.0 - acc)}")

    val treeModel = dtModel.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println(s"Learned classification tree model:\n ${treeModel.toDebugString}")

    /**
      * randomforest(随机森林)
      * 一棵树的生成肯定还是不如多棵树，因此就有了随机森林，解决决策树泛化能力弱的缺点
      * 采用Bagging策略(来源于bootstrap aggregation)产生不同的数据集
      * 1.样本的随机：从样本集中用Bootstrap随机选取n个样本
      * 2.特征的随机：从所有属性中随机选取K个属性，选择最佳分割属性作为节点建立CART决策树(泛化的理解，这里面也可以是其他类型的分类器，比如SVM、Logistics)
      * https://www.cnblogs.com/fionacai/p/5894142.html
      */
    //    训练随机森林模型
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    val pipelineRf = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val rfModel = pipelineRf.fit(trainingData)

    val predictionRf = rfModel.transform(testData)

    predictionRf.select("predictedLabel","label","features").show(5,false)

    val evalRf = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracyRf = evalRf.evaluate(predictionRf)
    println(s"Test Error = ${(1.0 - accuracyRf)}")

    val rf_model = rfModel.stages(2).asInstanceOf[RandomForestClassificationModel]
    println(s"Learned classification forest model:\n ${rf_model.toDebugString}")

    /**
      * GBDT(梯度提升决策树)
      * GBDT集成方法的一种，就是根据每次剩余的残差，即损失函数的值
      * 在残差减少的方向上建立一个新的模型的方法，直到达到一定拟合精度后停止。
      */
    //    GBDT模型训练
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")

//    indexer和gbdt组装管道
    val pipelineGbt = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

//    训练模型
    val modelGbt = pipelineGbt.fit(trainingData)

//    预测
    val predictionGbt = modelGbt.transform(testData)

    predictionGbt.select("predictedLabel", "label", "features").show(5,false)

//    评估模型
    val evalGbt = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracyGbt = evalGbt.evaluate(predictionGbt)
    println(s"Test Error = ${1.0 - accuracyGbt}")

    val gbtModel = modelGbt.stages(2).asInstanceOf[GBTClassificationModel]
    println(s"Learned classification GBT model:\n ${gbtModel.toDebugString}")

    val data = spark.read.format("libsvm")
      .load("/opt/modules/spark-2.3.1/data/mllib/sample_multiclass_classification_data.txt")

//    分割训练集和测试集
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
    val train = splits(0)
    val test = splits(1)

    /**
      * Multilayer perceptron classifier(多层感知分类器，神经网络分类)
      * 约定多层感知器网络各层节点个数
      * 输入层4个，两层隐含层分别为5个和4个
      * 输出层3个
      */
    val layers = Array[Int](4, 5, 4, 3)

//    创建训练模型并设定参数
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    val mpc = trainer.fit(train)

//    计算测试集预测
    val result = mpc.transform(test)
    val predictionMpc = result.select("prediction", "label")
    val evalMpc = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println(s"Test set accuracy = ${evalMpc.evaluate(predictionMpc)}")

    val mpcModel = mpc.asInstanceOf[MultilayerPerceptronClassificationModel]
    println(s"Learned classification MPC model weights:\n ${mpcModel.weights}")

    /**
      * Linear Support Vector Machine(线性支持向量机)
      *
      */
    val lsvc = new LinearSVC()
      .setMaxIter(10)
      .setRegParam(0.1)

    val lsvcModel = lsvc.fit(training)
    println(s"Coefficients: ${lsvcModel.coefficients} Intercept: ${lsvcModel.intercept}")
    val predictionLsvc = lsvcModel.transform(training).select("prediction","label")
    val evalLsvc = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println(s"Test set accuray = ${evalLsvc.evaluate(predictionLsvc)}")

    /**
      * One-vs-Rest classifier(多分类)
      * 对每个类别训练一个二元分类器（One-vs-all）适合于K个类别不是互斥
      * Softmax分类适合于K个类别是互斥的
      */
//    实例化LR分类器
    val classifier = new LogisticRegression()
      .setMaxIter(10)
      .setTol(1E-6)
      .setFitIntercept(true)

//    实例化One Vs Rest分类器
    val ovr = new OneVsRest().setClassifier(classifier)

//    训练多分类模型
    val ovrModel = ovr.fit(train)

    val predictionOvr = ovrModel.transform(test)

//    评估模型
    val evalOvr = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

//    计算测试集分类误差
    val accuracy = evalOvr.evaluate(predictionOvr)
    println(s"Test Error = ${1 - accuracy}")

    /**
      * 朴素贝叶斯分类
      */
    val nvb = new NaiveBayes()
      .fit(train)

    val predictionNvb = nvb.transform(test)
    predictionNvb.show(5,false)

//    评估贝叶斯分类
    val evalNvb = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracyNvb = evalNvb.evaluate(predictionNvb)
    println(s"Test set accuracy = $accuracyNvb")

  }
}
