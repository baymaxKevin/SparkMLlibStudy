package com.sparkMLlibStudy.model

import org.apache.spark.mllib.feature.{HashingTF, IDF, Word2Vec}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
/**
  * 以非常有名的文本分类数据集20 Newsgroups，由20个不同主题新闻消息组成
  * 训练集：20news-bydate-train，测试集：20news-bydate-test
  * 模型：tf-idf和word2vec
  */
object TextMineModel {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkCore")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
//    读取20news-bydate-train所有文件
    val rdd1 = sc.wholeTextFiles("/opt/data/20news-bydate-train/*")
    val text = rdd1.map{
      case (file, text) =>
        text
    }
    /**
      * 过滤单词，移除非单词字符，并且转为小写
      * \W：匹配热河非单词字符，等价于"[A-Za-z0-9_]"
      */
    val nonWordSplit = text.flatMap(f => f.split("""\W+""").map(_.toLowerCase))
//    过滤数字和包含数字的单词
    val regex = """[^0-9]*""".r
    val filterNumbers = nonWordSplit.filter(f => regex.pattern.matcher(f).matches())
//    println(filterNumbers.distinct().count())
//    println(filterNumbers.distinct().sample(true, 0.3,42)
//      .take(100).mkString(","))
    val tokenCounts = filterNumbers.map(f => (f, 1)).reduceByKey(_ + _)
    val oreringDesc = Ordering.by[(String, Int), Int](_._2)
//    println(tokenCounts.top(20)(oreringDesc).mkString("\n"))
//    移除停用词
    val stopwords = Set(
  "the","a","an","of","or","in","for","by","on","but", "is", "not", "with", "as", "was", "if",
  "they", "are", "this", "and", "it", "have", "from", "at", "my", "be", "that", "to"
)
    val tokenCountsFilteredStopwords = tokenCounts.filter{
      case (k, v) => !stopwords.contains(k)
    }
//    println(tokenCountsFilteredStopwords.top(20)(oreringDesc).mkString("\n"))
//    删除仅含有一个字符的单词
    val tokenCountsFilterSize =
      tokenCountsFilteredStopwords.filter{
        case (k, v) => k.size >= 2
      }
//    println(tokenCountsFilterSize.top(20)(oreringDesc).mkString("\n"))
//    除去频率低的单词
    val rareTokens = tokenCounts.filter{
      case (k, v) => v < 2
    }.map{ case (k, v) => k}.collect.toSet
    val tokenCountsFilteredAll = tokenCountsFilterSize
      .filter{case (k,v) => !rareTokens.contains(k)}
    val oreringAsc = Ordering.by[(String, Int), Int](_._2)
//    println(tokenCountsFilteredAll.top(20)(oreringDesc))
//    println(tokenCountsFilteredAll.count)
    /**
      * 过滤逻辑组合到一个函数中，并应用到RDD的每个文档中
      */
    def tokenize(line: String):Seq[String] = {
      line.split("""\W+""")
        .map(_.toLowerCase)
        .filter(token => regex.pattern.matcher(token).matches())
        .filterNot(token => stopwords.contains(token))
        .filterNot(token => rareTokens.contains(token))
        .filter(token => token.size >= 2)
        .toSeq
    }

//    println(text.flatMap(doc => tokenize(doc)).distinct.count())
    val tokens = text.map(doc => tokenize(doc))
//    println(tokens.first.take(20))

    /**
      * 训练tf-idf模型
      */
    val dim = math.pow(2, 18).toInt
    val hashingTF = new HashingTF(dim)

    val tf = hashingTF.transform(tokens)
//    缓存数据
    tf.cache()
    val v = tf.first().asInstanceOf[SV]
//    println(v.size)
//    println(v.values.size)
//    println(v.values.take(10).toSeq)
//    println(v.indices.take(10).toSeq)

    /**
      * 通过创建新的IDF实例，调用fit方法
      * 利用词频向量作为输入对文库中每个单词计算逆向文本频率
      * 使用IDF的transform方法转换词频向量为TF-IDF向量
      */
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    val v2 = tfidf.first().asInstanceOf[SV]
//    println(v2.values.size)
//    println(v2.values.take(10).toSeq)
//    println(v2.indices.take(10).toSeq)
    /**
      * 预估曲棍球新闻组随即选择新闻比较相似
      */
    val hockeyText = rdd1.filter{
      case (file, text) => file.contains("hockey")
    }
    val hockeyTF = hockeyText.mapValues(doc => hashingTF
      .transform(tokenize(doc)))
    val hockeyTFIDF = idf.transform(hockeyTF.map(_._2))

//    计算余弦相似度
    import breeze.linalg._
    val hockey1 = hockeyTFIDF.sample(true, 0.1, 28).first().asInstanceOf[SV]
    val breeze1 = new SparseVector(hockey1.indices, hockey1.values, hockey1.size)
    val hockey2 = hockeyTFIDF.sample(true,0.1,10).first().asInstanceOf[SV]
    val breeze2 = new SparseVector(hockey2.indices, hockey2.values, hockey2.size)
    val cosineSim = breeze1.dot(breeze2)/(norm(breeze1) * norm(breeze2))
//    println(cosineSim)

    /**
      * word2Vec
      */
    val word2vec = new Word2Vec()
    word2vec.setSeed(28)
    val word2vecModel = word2vec.fit(tokens)

    word2vecModel.findSynonyms("hockey", 20).foreach(println)
  }
}
