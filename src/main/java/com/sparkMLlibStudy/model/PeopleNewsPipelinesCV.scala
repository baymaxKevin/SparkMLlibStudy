package com.sparkMLlibStudy.model

import java.util.Date

import com.sparkMLlibStudy.model.PeopleNewsPipelines.HanLPTokenizer
import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

/**
  * @Author: JZ.lee
  * @Description: 资讯分类：
  *              1. pipelines组装特征工程和model
  *              2. model对比
  *              3. cv参数寻优
  * @Date: 18-9-18 下午2:12
  * @Modified By:
  */
object PeopleNewsPipelinesCV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PeopleNews")
      .master("local[2]")
      .config("spark.some.config.option","some-value")
      .getOrCreate()

    val news = spark.read.format("CSV").option("header","true").load("/opt/data/peopleNews.csv")
    val peopleWebNews = news.filter(news("title").isNotNull && news("created_time").isNotNull && news("tab").isNotNull && news("content").isNotNull && news("source").isNotNull)
    val peopleNews = peopleWebNews.filter(peopleWebNews("tab").isin("国际","军事","财经","金融","时政","法制","社会"))

    val indexer = new StringIndexer()
      .setInputCol("tab")
      .setOutputCol("label")
      .fit(peopleNews)

    val segmenter = new HanLPTokenizer()
      .setInputCol("content")
      .setOutputCol("tokens")

    val stopwords = spark.read.textFile("/opt/data/stopwordsCH.txt").collect()

    val remover = new StopWordsRemover()
      .setStopWords(stopwords)
      .setInputCol("tokens")
      .setOutputCol("removed")

    val vectorizer = new CountVectorizer()
      .setInputCol("removed")
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(40)
      .setTol(1e-7)
      .setLabelCol("label")
      .setFeaturesCol("features")

    val converts = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictionTab")
      .setLabels(indexer.labels)

    val Array(train,test) = peopleNews.randomSplit(Array(0.8,0.2),12L)

    val lrStartTime = new Date().getTime
    val lrPipeline = new Pipeline()
        .setStages(Array(indexer,segmenter,remover,vectorizer,lr,converts))
//    交叉验证参数设定和模型
    val lrParamGrid = new ParamGridBuilder()
      .addGrid(vectorizer.vocabSize,Array(1<<10,1<<18))
      .addGrid(vectorizer.minDF,Array(1.0,2.0))
      .addGrid(lr.regParam,Array(0.1,0.01))
      .addGrid(lr.elasticNetParam,Array(0.1,0.0))
      .build()

    val lrCv = new CrossValidator()
      .setEstimator(lrPipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(lrParamGrid)
      .setNumFolds(2)
      .setParallelism(4)

    val lrModel = lrCv.fit(train)
    val lrValiad = lrModel.transform(train)
    val lrPredictions = lrModel.transform(test)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracyLrt = evaluator.evaluate(lrValiad)
    println(s"逻辑回归验证集分类准确率 = $accuracyLrt")
    val accuracyLrv = evaluator.evaluate(lrPredictions)
    println(s"逻辑回归测试集分类准确率 = $accuracyLrv")
    val lrEndTime = new Date().getTime
    val lrCostTime = (lrEndTime - lrStartTime)/lrParamGrid.length
    println(s"逻辑回归分类耗时：$lrCostTime")
//    获取最优模型
    val bestLrModel = lrModel.bestModel.asInstanceOf[PipelineModel]
    val bestLrVectorizer = bestLrModel.stages(3).asInstanceOf[CountVectorizerModel]
    val blvv = bestLrVectorizer.getVocabSize
    val blvm = bestLrVectorizer.getMinDF
    val bestLr = bestLrModel.stages(4).asInstanceOf[LogisticRegressionModel]
    val blr = bestLr.getRegParam
    val ble = bestLr.getElasticNetParam
    println(s"countVectorizer模型最优参数：\ngetVocabSize= $blvv，minDF = $blvm\n逻辑回归模型最优参数：\nregParam = $blr，elasticNetParam = $ble")

//    训练决策树模型
    val dtStartTime = new Date().getTime
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setSeed(123456L)

    val dtPipeline = new Pipeline()
      .setStages(Array(indexer,segmenter,remover,vectorizer,dt,converts))

//    交叉验证参数设定和模型
    val dtParamGrid = new ParamGridBuilder()
      .addGrid(vectorizer.vocabSize,Array(1<<10,1<<18))
      .addGrid(vectorizer.minDF,Array(1.0,2.0))
      .addGrid(dt.impurity,Array("entropy","gini"))
      .addGrid(dt.maxDepth,Array(5,10))
      .addGrid(dt.maxBins,Array(32,500))
      .addGrid(dt.minInfoGain,Array(0.1,0.01))
      .addGrid(dt.minInstancesPerNode,Array(5,10))
      .build()

    val dtCv = new CrossValidator()
      .setEstimator(dtPipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(dtParamGrid)
      .setNumFolds(2)
      .setParallelism(7)

    val dtModel = dtCv.fit(train)
    val dtValiad = dtModel.transform(train)
    val dtPredictions = dtModel.transform(test)
    val accuracyDtt = evaluator.evaluate(dtValiad)
    println(s"决策树验证集分类准确率 = $accuracyDtt")
    val accuracyDtv = evaluator.evaluate(dtPredictions)
    println(s"决策树测试集分类准确率 = $accuracyDtv")
    val dtEndTime = new Date().getTime
    val dtCostTime = (dtEndTime - dtStartTime)/dtParamGrid.length
    println(s"决策树分类耗时：$dtCostTime")

//    获取最优模型
    val bestDtModel = dtModel.bestModel.asInstanceOf[PipelineModel]
    val bestDtVectorizer = bestDtModel.stages(3).asInstanceOf[CountVectorizerModel]
    val bdvv = bestDtVectorizer.getVocabSize
    val bdvm = bestDtVectorizer.getMinDF
    val bestDt = bestDtModel.stages(4).asInstanceOf[DecisionTreeClassificationModel]
    val bdi = bestDt.getImpurity
    val bdmd = bestDt.getMaxDepth
    val bdmb = bestDt.getMaxBins
    val bdmig = bestDt.getMinInfoGain
    val bdmipn = bestDt.getMinInstancesPerNode
    println(s"countVectorizer模型最优参数：\nvocabSize = $bdvv，minDF = $bdvm\n决策树分类模型最优参数：\nmpurity = $bdi，maxDepth = $bdmd，maxBins = $bdmb，minInfoGain = $bdmig，minInstancesPerNode = $bdmipn")

//    训练随机森林模型
    val rfStartTime = new Date().getTime
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setSeed(123456L)

    val rfPipeline = new Pipeline()
      .setStages(Array(indexer,segmenter,remover,vectorizer,rf,converts))

//    交叉验证参数设定和模型
    val rfParamGrid = new ParamGridBuilder()
      .addGrid(vectorizer.vocabSize,Array(1<<10,1<<18))
      .addGrid(vectorizer.minDF,Array(1.0,2.0))
      .addGrid(rf.impurity,Array("entropy","gini"))
      .addGrid(rf.maxDepth,Array(5,10))
      .addGrid(rf.maxBins,Array(32,500))
      .addGrid(rf.minInfoGain,Array(0.1,0.01))
      .addGrid(rf.minInstancesPerNode,Array(5,10))
      .addGrid(rf.numTrees,Array(20,50))
      .addGrid(rf.subsamplingRate,Array(0.2,0.1))
      .build()

    val rfCv = new CrossValidator()
      .setEstimator(rfPipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(rfParamGrid)
      .setNumFolds(2)
      .setParallelism(9)

    val rfModel = rfCv.fit(train)
    val rfValiad = rfModel.transform(train)
    val rfPredictions = rfModel.transform(test)
    val accuracyRft = evaluator.evaluate(rfValiad)
    println(s"随机森林验证集分类准确率为：$accuracyRft")
    val accuracyRfv = evaluator.evaluate(rfPredictions)
    println(s"随机森林测试集分类准确率为：$accuracyRfv")
    val rfEndTime = new Date().getTime
    val rfCostTime = (rfEndTime - rfStartTime)/rfParamGrid.length
    println(s"随机森林分类耗时：$rfCostTime")

//    获取最优模型
    val bestRfModel = rfModel.bestModel.asInstanceOf[PipelineModel]
    val bestRfVectorizer = bestRfModel.stages(3).asInstanceOf[CountVectorizerModel]
    val brvv = bestRfVectorizer.getVocabSize
    val brvm = bestRfVectorizer.getMinDF
    val bestRf = bestRfModel.stages(4).asInstanceOf[RandomForestClassificationModel]
    val bri = bestRf.getImpurity
    val brmd = bestRf.getMaxDepth
    val brmb = bestRf.getMaxBins
    val brmig = bestRf.getMinInfoGain
    val brmipn = bestRf.getMinInstancesPerNode
    val brnt = bestRf.getNumTrees
    val brsr = bestRf.getSubsamplingRate
    println(s"countVectorizer模型最优参数：\nvocabSize = $brvv，eminDF = $brvm\n随机森林分类模型最优参数：\nmpurity = $bri，maxDepth = $brmd，maxBins = $brmb，minInfoGain = $brmig，minInstancesPerNode = $brmipn，numTrees = $brnt，subsamplingRate = $brsr")

    val outputLayers = peopleNews.select("tab").distinct().count().toInt

//    xgboost训练模型
    val xgbStartTime = new Date().getTime
    val xgb = new XGBoostClassifier()
      .setObjective("multi:softprob")
      .setNumClass(outputLayers)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setNumWorkers(1)

    val xgbPipeline = new Pipeline()
      .setStages(Array(indexer,segmenter,remover,vectorizer,xgb,converts))

//    交叉验证参数设定和模型
    val xgbParamGrid = new ParamGridBuilder()
      .addGrid(vectorizer.vocabSize,Array(1<<10,1<<18))
      .addGrid(vectorizer.minDF,Array(1.0,2.0))
      .addGrid(xgb.eta,Array(0.3,0.1))
      .addGrid(xgb.maxDepth,Array(6,10))
      .addGrid(xgb.numRound,Array(10,100))
      .addGrid(xgb.alpha,Array(0.1,0.0))
      .build()

    val xgbCv = new CrossValidator()
      .setEstimator(xgbPipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(xgbParamGrid)
      .setNumFolds(2)
      .setParallelism(6)

    val xgbModel = xgbCv.fit(train)
    val xgbValiad = xgbModel.transform(train)
    val xgbPredictions = xgbModel.transform(test)
    val accuracyXgbt = evaluator.evaluate(xgbValiad)
    println(s"xgboost验证集分类准确率为：$accuracyXgbt")
    val accuracyXgbv = evaluator.evaluate(xgbPredictions)
    println(s"xgboost测试集分类准确率为：$accuracyXgbv")
    val xgbEndTime = new Date().getTime
    val xgbCostTime = (xgbEndTime - xgbStartTime)/xgbParamGrid.length
    println(s"xgboost分类耗时：$xgbCostTime")


//    获取最优模型
    val bestXgbModel = xgbModel.bestModel.asInstanceOf[PipelineModel]
    val bestXgbVectorizer = bestXgbModel.stages(3).asInstanceOf[CountVectorizerModel]
    val bxvv = bestXgbVectorizer.getVocabSize
    val bxvm = bestXgbVectorizer.getMinDF
    val bestXgb = bestXgbModel.stages(4).asInstanceOf[XGBoostClassificationModel]
    val bxe = bestXgb.getEta
    val bxmd = bestXgb.getMaxDepth
    val bxnr = bestXgb.getNumRound
    val bxa = bestXgb.getAlpha
    println(s"countVectorizer模型最优参数：\nvocabSize = $bxvv，minDF = $bxvm\nXGBOOST分类模型最优参数：\neta = $bxe，maxDepth = $bxmd，numRound = $bxnr，alpha = $bxa")

//    朴素贝叶斯分类
    val nvbStartTime = new Date().getTime
    val nvb = new NaiveBayes()
    val nvbPipeline = new Pipeline()
      .setStages(Array(indexer,segmenter,remover,vectorizer,nvb,converts))

//    交叉验证参数设定和模型
    val nvbParamGrid = new ParamGridBuilder()
      .addGrid(vectorizer.vocabSize,Array(1<<10,1<<18))
      .addGrid(vectorizer.minDF,Array(1.0,2.0))
      .addGrid(nvb.smoothing,Array(1, 0.5))
      .build()

    val nvbCv = new CrossValidator()
      .setEstimator(nvbPipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(nvbParamGrid)
      .setNumFolds(2)
      .setParallelism(3)

    val nvbModel = nvbCv.fit(train)
    val nvbValiad = nvbModel.transform(train)
    val nvbPredictions = nvbModel.transform(test)
    val accuracyNvbt = evaluator.evaluate(nvbValiad)
    println(s"朴素贝叶斯验证集分类准确率：$accuracyNvbt")
    val accuracyNvbv = evaluator.evaluate(nvbPredictions)
    println(s"朴素贝叶斯测试集分类准确率：$accuracyNvbv")
    val nvbEndTime = new Date().getTime
    val nvbCostTime = (nvbEndTime - nvbStartTime)/nvbParamGrid.length
    println(s"朴素贝叶斯分类耗时：$nvbCostTime")

//    获取最优模型
    val bestNvbModel = nvbModel.bestModel.asInstanceOf[PipelineModel]
    val bestNvbVectorizer = bestNvbModel.stages(3).asInstanceOf[CountVectorizerModel]
    val bnvv = bestNvbVectorizer.getVocabSize
    val bnvm = bestNvbVectorizer.getMinDF
    val bestNvb = bestNvbModel.stages(4).asInstanceOf[NaiveBayesModel]
    val bns = bestNvb.getSmoothing
    println(s"countVectorizer模型最优参数：\nvocabSize = $bnvv，minDF = $bnvm\n朴素贝叶斯分类模型最优参数：\nsmoothing = $bns")
  }
}
