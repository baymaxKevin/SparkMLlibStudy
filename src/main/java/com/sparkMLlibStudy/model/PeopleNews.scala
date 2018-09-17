package com.sparkMLlibStudy.model

import com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer
import com.hankcs.hanlp.seg.NShort.NShortSegment
import com.hankcs.hanlp.seg.Segment
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.{IndexTokenizer, NLPTokenizer, SpeedTokenizer, StandardTokenizer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StopWordsRemover, StringIndexer}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-9-14 上午9:35
  * @Modified By:
  */
object PeopleNews {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PeopleNews")
      .master("local[2]")
      .config("spark.some.config.option","some-value")
      .getOrCreate()

    val news = spark.read.format("CSV").option("header","true").load("/opt/data/peopleNews.csv")
    news.show(5,false)
//    println(news.where(news("title").isNotNull && news("created_time").isNotNull && news("label").isNotNull && news("content").isNotNull && news("source").isNotNull).count())
    val peopleWebNews = news.filter(news("title").isNotNull && news("created_time").isNotNull && news("tab").isNotNull && news("content").isNotNull && news("source").isNotNull)
    println("过滤完整资讯条数为：" + peopleWebNews.count())
    println("各频道资讯条数为：")
    peopleWebNews.groupBy("tab").count().show(false)
    peopleWebNews.show(5, false)
    val peopleNews = peopleWebNews.filter(peopleWebNews("tab").isin("国际","军事","财经","金融","时政","法制","社会"))

    val indexer = new StringIndexer()
      .setInputCol("tab")
      .setOutputCol("label")
      .fit(peopleNews)

    val indDF = indexer.transform(peopleNews)
    indDF.groupBy("tab","label").count().show(false)
    indDF.show(5,false)

    val segmenter = new Segmenter(spark)
      .setSegmentType("StandardSegment")
      .enableNature(false)
      .setInputCol("content")
      .setOutputCol("tokens")

    val segDF = segmenter.transform(indDF)
    segDF.show(5,false)

    val stopwords = spark.read.textFile("/opt/data/stopwordsCH.txt").collect()

    val remover = new StopWordsRemover()
      .setStopWords(stopwords)
      .setInputCol("tokens")
      .setOutputCol("removed")
    val removedDF = remover.transform(segDF)
    removedDF.show(5,false)

    val vectorizer = new CountVectorizer()
      .setVocabSize(2000)
      .setInputCol("removed")
      .setOutputCol("features")
      .fit(removedDF)
    val vectorDF = vectorizer.transform(removedDF)

    vectorDF.select("label","features").show(5,false)

    val Array(train,test) = vectorDF.randomSplit(Array(0.8,0.2),15L)
    train.persist()
    val lr = new LogisticRegression()
      .setMaxIter(40)
      .setRegParam(0.2)
      .setElasticNetParam(0.0)
      .setTol(1e-7)
      .setLabelCol("label")
      .setFeaturesCol("features")
      .fit(train)
    train.unpersist()
//    打印逻辑回归的系数和截距
    println(s"Coefficients: ${lr.coefficientMatrix}")
    println(s"Intercept: ${lr.interceptVector} ")
    val trainingSummary = lr.summary

//    获取每次迭代目标,每次迭代的损失,会逐渐减少
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    /**
      * 二分类常用指标：
      * True Positive(真正,TP)=>将正类预测为正类
      * True Negative(真负,TN)=>将负类预测为负类
      * False Positive(假正,FP)=>将负类预测为正类，误报
      * False Nagative(假负,FN)=>将正类预测为负类，漏报
      * True positive rate: TPR=TP/(TP+FN)
      * false positive rate: FPR=FP/(FP+TN)
      * Precision(精确率):预测为1的结果中预测正确的概率
      * Recall(召回率):标签为1样本被预测正确概率，Recall=TPR
      * Precision和Recall在某些场景下是互斥的，需要一定的约束变量来控制，引入F-score
      * F=(a^2+1)P*R/a^2*(P+R)
      * ROC曲线: 横坐标是FRP，纵坐标是TPR，TPR越大，FPR越小，分类效果较好
      * ROC曲线并不能完美的表征二分类器的分类性能，如何评价?
      * AUC：ROC曲线下的面积
      */
    println("验证集各标签误报率(FPR):")
    trainingSummary.falsePositiveRateByLabel
      .zipWithIndex.foreach { case (rate, label) =>
      println(s"label $label: $rate")
    }

    println("验证集各标签真分类率(TPR):")
    trainingSummary.truePositiveRateByLabel.zipWithIndex
      .foreach { case (rate, label) =>
        println(s"label $label: $rate")
      }

    println("验证集各标签分类正确率:")
    trainingSummary.precisionByLabel.zipWithIndex
      .foreach { case (prec, label) =>
        println(s"label $label: $prec")
      }

    println("验证集各标签召回率:")
    trainingSummary.recallByLabel.zipWithIndex.foreach {
      case (rec, label) =>
        println(s"label $label: $rec")
    }


    println("验证集各标签F值:")
    trainingSummary.fMeasureByLabel.zipWithIndex.foreach
    { case (f, label) =>
      println(s"label $label: $f")
    }

    val accuracyLtr = trainingSummary.accuracy
    val falsePositiveRateLtr =
      trainingSummary.weightedFalsePositiveRate
    val truePositiveRateLtr =
      trainingSummary.weightedTruePositiveRate
    val fMeasureLtr = trainingSummary.weightedFMeasure
    val precisionLtr = trainingSummary.weightedPrecision
    val recallLtr = trainingSummary.weightedRecall
    println(s"分类准确率(Precision): $accuracyLtr\n误报率(FPR): $falsePositiveRateLtr\n真正类率(TPR): $truePositiveRateLtr\n" +
      s"F值(F-measure): $fMeasureLtr\n分类准确率(Precision): $precisionLtr \n召回率(Recall): $recallLtr")

    val predictions = lr.transform(test)
    val converts = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictionTab")
      .setLabels(indDF.schema("label").metadata.getMetadata("ml_attr").getStringArray("vals"))

    val predTab = converts.transform(predictions)
    predTab.select("prediction","predictionTab","label","tab","probability").show(5,false)
    val lrv = lr.evaluate(test)
    println("测试集各标签误报率(FPR):")
    lrv.falsePositiveRateByLabel.zipWithIndex.foreach{
      case (rate, label) =>println(s"label $label: $rate")
    }
    println("测试集各标签真正率(TPR):")
    lrv.truePositiveRateByLabel.zipWithIndex.foreach{
      case (rate, label) =>println(s"label $label: $rate")
    }
    println("测试集各标签准确率(Precision):")
    lrv.precisionByLabel.zipWithIndex.foreach{
      case (rate, label) =>println(s"label $label: $rate")
    }
    println("测试集各标签召回率(Recall):")
    lrv.recallByLabel.zipWithIndex.foreach{
      case (rate, label) =>println(s"label $label: $rate")
    }
    println("测试集各标签F1值:")
    lrv.fMeasureByLabel.zipWithIndex.foreach{
      case (f, label) =>println(s"label $label:$f")
    }
    val accuracyLrv = lrv.accuracy
    val truePositiveRateLrv = lrv.weightedTruePositiveRate
    val falsePositiveRateLrv = lrv.weightedFalsePositiveRate
    val fMeasureLrv = lrv.weightedFMeasure
    val precisionLrv = lrv.weightedPrecision
    val recallLrv = lrv.weightedRecall
    println(s"分类准确率(Precision): $accuracyLrv\n误报率(FPR): $falsePositiveRateLrv\n真正类率(TPR): $truePositiveRateLrv\n" +
      s"F值(F-measure): $fMeasureLrv\n分类准确率(Precision): $precisionLrv \n召回率(Recall): $recallLrv")
  }

  /**
    * HanLP分词
    * @param spark
    * @param uid
    */
  class Segmenter(spark:SparkSession,uid:String) extends Serializable {

    private var inputCol = ""
    private var outputCol = ""
    private var segmentType = "StandardTokenizer"
    private var enableNature = false

    def setInputCol(value:String):this.type = {
      this.inputCol = value
      this
    }

    def setOutputCol(value:String):this.type = {
      this.outputCol = value
      this
    }

    def setSegmentType(value:String):this.type = {
      this.segmentType = value
      this
    }

    def enableNature(value:Boolean):this.type = {
      this.enableNature = value
      this
    }

    def this(spark:SparkSession) = this(spark, Identifiable.randomUID("segment"))

    def transform(dataset: DataFrame) : DataFrame = {
      var segment : Segment = null
      segmentType match {
        case "NShortSegment" =>
          segment = new MyNShortSegment()
        case "CRFSegment" =>
          segment = new MyCRFLexicalAnalyzer()
        case _=>
      }

      val tokens = dataset.select(inputCol).rdd.map{ case Row(line:String) =>
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
            terms = segment.seg(line)
          case _=>
            println("分词类型错误！")
            System.exit(1)
        }

        val termSeq = terms.map(term =>
          if(this.enableNature) term.toString else term.word
        )
        (line, termSeq)
      }

      import spark.implicits._
      val tokensSet = tokens.toDF(inputCol + "#1", outputCol)

      dataset.join(tokensSet, dataset(inputCol) === tokensSet(inputCol + "#1")).drop(inputCol + "#1")
    }
  }

  /**
    * HanLP自定义分词方法，实现序列化
    */
  class MyCRFLexicalAnalyzer() extends CRFLexicalAnalyzer with Serializable{}
  class MyNShortSegment() extends NShortSegment with Serializable{}
}
