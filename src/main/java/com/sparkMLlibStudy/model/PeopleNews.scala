package com.sparkMLlibStudy.model

import com.hankcs.hanlp.model.crf.CRFLexicalAnalyzer
import com.hankcs.hanlp.seg.NShort.NShortSegment
import com.hankcs.hanlp.seg.Segment
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.{IndexTokenizer, NLPTokenizer, SpeedTokenizer, StandardTokenizer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StopWordsRemover, StringIndexer}
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
    val peopleNews = news.filter(news("title").isNotNull && news("created_time").isNotNull && news("tab").isNotNull && news("content").isNotNull && news("source").isNotNull)

    peopleNews.show(5, false)

    val indexer = new StringIndexer()
      .setInputCol("tab")
      .setOutputCol("label")
      .fit(peopleNews)

    val indDF = indexer.transform(peopleNews)
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

    val predictions = lr.transform(test)
    predictions.select("prediction","label","probability").show(5,false)
    val accuracyLr = lr.evaluate(test).accuracy
    val weightedPrecisionLr = lr.evaluate(test).weightedPrecision
    val weightedRecallLr = lr.evaluate(test).weightedRecall
    println(s"accuray: ${accuracyLr}\nweightedPrecision:${weightedPrecisionLr}\nweightedRecallLr:${weightedRecallLr}")

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
