package com.sparkMLlibStudy.model

import com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer
import com.hankcs.hanlp.seg.NShort.NShortSegment
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.{IndexTokenizer, NLPTokenizer, SpeedTokenizer, StandardTokenizer}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Pipeline, UnaryTransformer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StringType}

/**
  * @Author: JZ.lee
  * @Description: 资讯分类：
  *              1. pipelines组装特征工程和model
  *              2. model对比
  * @Date: 18-9-18 下午2:12
  * @Modified By:
  */
object PeopleNewsPipelines {
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
      .enableNature(false)
      .setSegmentType("StandardSegment")

    val stopwords = spark.read.textFile("/opt/data/stopwordsCH.txt").collect()

    val remover = new StopWordsRemover()
      .setStopWords(stopwords)
      .setInputCol("tokens")
      .setOutputCol("removed")

    val vectorizer = new CountVectorizer()
      .setVocabSize(1000)
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

    val lrPipeline = new Pipeline()
        .setStages(Array(indexer,segmenter,remover,vectorizer,lr,converts))

    val Array(train,test) = peopleNews.randomSplit(Array(0.8,0.2),12L)

    val lrModel = lrPipeline.fit(train)
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

//    训练决策树模型
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setImpurity("entropy") // 不纯度
      .setMaxBins(1000) // 离散化"连续特征"的最大划分数
      .setMaxDepth(10) // 树的最大深度
      .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
      .setMinInstancesPerNode(5) //每个节点包含的最小样本数
      .setSeed(123456L)

    val dtPipeline = new Pipeline()
      .setStages(Array(indexer,segmenter,remover,vectorizer,dt,converts))

    val dtModel = dtPipeline.fit(train)
    val dtValiad = dtModel.transform(train)
    val dtPredictions = dtModel.transform(test)
    val accuracyDtt = evaluator.evaluate(dtValiad)
    println(s"决策树验证集分类准确率 = $accuracyDtt")
    val accuracyDtv = evaluator.evaluate(dtPredictions)
    println(s"决策树测试集分类准确率 = $accuracyDtv")

    //    训练随机森林模型
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setImpurity("entropy") // 不纯度
      .setMaxBins(1000) // 离散化"连续特征"的最大划分数
      .setMaxDepth(10) // 树的最大深度
      .setMinInfoGain(0.01) //一个节点分裂的最小信息增益，值为[0,1]
      .setMinInstancesPerNode(5) //每个节点包含的最小样本数
      .setNumTrees(100)
      .setSeed(123456L)

    val rfPipeline = new Pipeline()
      .setStages(Array(indexer,segmenter,remover,vectorizer,rf,converts))
    val rfModel = rfPipeline.fit(train)
    val rfValiad = rfModel.transform(train)
    val rfPredictions = rfModel.transform(test)
    val accuracyRft = evaluator.evaluate(rfValiad)
    println(s"随机森林验证集分类准确率为：$accuracyRft")
    val accuracyRfv = evaluator.evaluate(rfPredictions)
    println(s"随机森林测试集分类准确率为：$accuracyRfv")

    //    GBDT模型训练
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setFeatureSubsetStrategy("auto")

//    val gbtPipeline = new Pipeline()
//      .setStages(Array(indexer,segmenter,remover,vectorizer,gbt,converts))
//    val gbtModel = gbtPipeline.fit(train)
//    val gbtValiad = gbtModel.transform(train)
//    val gbtPredictions = gbtModel.transform(test)
//    val accuracyGbtt = evaluator.evaluate(gbtValiad)
//    println(s"梯度提升决策树验证集分类准确率：$accuracyGbtt")
//    val accuracyGbtv = evaluator.evaluate(gbtPredictions)
//    println(s"梯度提升决策树测试集分类准确率：$accuracyGbtv")

//    多层感知分类器
    val inputLayers = vectorizer.getVocabSize
    val hideLayer1 = Math.round(Math.log(inputLayers)/Math.log(2)).toInt
    val outputLayers = peopleNews.select("tab").distinct().count().toInt
    val hideLayer2 = Math.round(Math.sqrt(inputLayers + outputLayers) + 1).toInt
    val layers = Array[Int](inputLayers, hideLayer1, hideLayer2, outputLayers)
    val mpc = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setTol(1e-7)
      .setMaxIter(100)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setSeed(1234L)

    val mpcPipeline = new Pipeline()
      .setStages(Array(indexer,segmenter,remover,vectorizer,mpc,converts))
    val mpcModel = mpcPipeline.fit(train)
    val mpcValiad = mpcModel.transform(train)
    val mpcPredictions = mpcModel.transform(test)
    val accuracyMpct = evaluator.evaluate(mpcValiad)
    println(s"多层感知分类器验证集分类准确率：$accuracyMpct")
    val accuracyMpcv = evaluator.evaluate(mpcPredictions)
    println(s"多层感知分类器测试集分类准确率：$accuracyMpcv")

//    线性支持向量机
    val lsvc = new LinearSVC()
      .setMaxIter(10)
      .setTol(1e-7)

//    val lsvcPipeline = new Pipeline()
//      .setStages(Array(indexer,segmenter,remover,vectorizer,lsvc,converts))
//    val lsvcModel = lsvcPipeline.fit(train)
//    val lsvcValiad = lsvcModel.transform(train)
//    val lsvcPredictions = lsvcModel.transform(test)
//    val accuracyLsvct = evaluator.evaluate(lsvcValiad)
//    println(s"线性支持向量机验证集分类准确率：$accuracyLsvct")
//    val accuracyLsvcv = evaluator.evaluate(lsvcPredictions)
//    println(s"线性支持向量机测试集分类准确率：$accuracyLsvcv")

//    朴素贝叶斯分类
    val nvb = new NaiveBayes()
    val nvbPipeline = new Pipeline()
      .setStages(Array(indexer,segmenter,remover,vectorizer,nvb,converts))
    val nvbModel = nvbPipeline.fit(train)
    val nvbValiad = nvbModel.transform(train)
    val nvbPredictions = nvbModel.transform(test)
    val accuracyNvbt = evaluator.evaluate(nvbValiad)
    println(s"朴素贝叶斯验证集分类准确率：$accuracyNvbt")
    val accuracyNvbv = evaluator.evaluate(nvbPredictions)
    println(s"朴素贝叶斯测试集分类准确率：$accuracyNvbv")
  }

  class HanLPTokenizer(override val uid:String) extends UnaryTransformer[String, Seq[String], HanLPTokenizer] {

    private var segmentType = "StandardTokenizer"
    private var enableNature = false

    def setSegmentType(value:String):this.type = {
      this.segmentType = value
      this
    }

    def enableNature(value:Boolean):this.type  = {
      this.enableNature = value
      this
    }

    def this() = this(Identifiable.randomUID("HanLPTokenizer"))

    override protected def createTransformFunc: String => Seq[String] = {
      hanLP
    }

    private def hanLP(line:String): Seq[String] = {
      var terms: Seq[Term] = Seq()
      import collection.JavaConversions._
      segmentType match {
        case "StandardSegment" =>
          terms = StandardTokenizer.segment(line)
        case "NLPSegment" =>
          terms = NLPTokenizer.segment(line)
        case "IndexSegment" =>
          terms = IndexTokenizer.segment(line)
        case "SpeedSegment" =>
          terms = SpeedTokenizer.segment(line)
        case "NShortSegment" =>
          terms = new NShortSegment().seg(line)
        case "CRFlexicalAnalyzer" =>
          terms = new CRFLexicalAnalyzer().seg(line)
        case _ =>
          println("分词类型错误！")
          System.exit(1)
      }
      val termSeq = terms.map(term =>
      if(this.enableNature) term.toString else term.word)
      termSeq
    }

    override protected def validateInputType(inputType: DataType): Unit = {
      require(inputType == DataTypes.StringType,
        s"Input type must be string type but got $inputType.")
    }
    override protected def outputDataType: DataType = new ArrayType(StringType, true)
  }
}
