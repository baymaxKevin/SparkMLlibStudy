package org.apache.spark

import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-8-20 下午5:10
  * @Modified By:
  */
object TextMineML {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TextMineML")
      .config("spark.some.config.option", "some-value")
      .master("local[2]")
      .getOrCreate()

    val files = spark.sparkContext
      .wholeTextFiles("/opt/data/20news-bydate-train/*")
    import spark.implicits._
    val sentences = files.map(f=>News(f._1.split("/").slice(4, 6).mkString("."),f._2)).toDF("name","content").as[News]
    //    ml中分词器分词(默认空白符分割，改写为非数字/字母/下划线分割，并过滤掉含有数字词语)
    val tokenizer = new Tokenizer(){
      override protected def createTransformFunc: String => Seq[String] = {
        _.toLowerCase.split("\\W+").filter("""[^0-9]*""".r.pattern.matcher(_).matches())
      }
    }.setInputCol("content").setOutputCol("raw")
    val raw = tokenizer.transform(sentences)
    //    去除停用词
    val remover = new StopWordsRemover().setInputCol("raw").setOutputCol("words")
    //    打印停用词
    println(remover.getStopWords.mkString("[",",","]"))
    val words = remover.transform(raw)
    words.select("name","words").show(5,false)
//    //    tf计算
//    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(math.pow(2, 18).toInt)
//    val features = hashingTF.transform(words)
//    //    idf计算
//    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//    val idfModel = idf.fit(features)
//    val rescaled:Dataset[Row] = idfModel.transform(features)
//    rescaled.select("name","words","features").show(5, false)
//
//    /**
//      * 文本相似度检验
//      * 从新闻组中随即选取1篇新闻并与其他新闻比较选出topN
//      */
//    val selected = rescaled.sample(true, 0.2, 8).select("name","features").first()
//    val selected_feature:Vector = selected.getAs(1)
//    val sv1 = Vectors.norm(selected_feature, 2.0)
//    println("随机选取news=" + selected.getAs[String]("name"))
//    val sim = rescaled.select("name","features").map(row =>{
//      val id = row.getAs[String]("name")
//      val feature:Vector = row.getAs(1)
//      val sv2 = Vectors.norm(feature, 2.0)
//      val similarity = org.apache.spark.ml.linalg.BLAS.dot(selected_feature.toSparse, feature.toSparse)/(sv1 * sv2)
//      SimilarityData(id, similarity)
//    }).toDF("id","similarity").as[SimilarityData]
//    println("与之最相似top100新闻为:")
//    sim.orderBy("similarity").select("id","similarity").limit(100).show(false)

    /**
      * word2vec
      * 2013年google开源的一款将词表征为实数值向量的工具，利用深度学习思想，把对文本内容处理简化为K维向量空间的向量运算，而向量空间的相似度表示文本寓意相似度
      */
    val word2vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(1)
    val model = word2vec.fit(words)
    val result = model.transform(words)
    model.findSynonyms("space", 20).collect().foreach(println)
//    result.collect().foreach{
//      case Row(text:Seq[_], features:Vector) =>
//        println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
//    }
  }
  case class News(var name:String, var content:String)
  case class Rescaled(var name:String, var words:String, var rawFeatures:String)
  case class SimilarityData(var id:String, var similarity:Double)
}
