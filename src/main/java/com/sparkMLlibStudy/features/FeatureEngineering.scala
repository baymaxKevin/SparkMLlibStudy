package com.sparkMLlibStudy.features

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.DataFrame

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-11-22 下午4:39
  * @Modified By: 特征工程处理
  */
class FeatureEngineering {
  /**
    * Array(user_embedding,item_embedding)->vector(user_embedding,item_embedding)
    * @param samples
    * @return
    */
  def transferArray2Vector(samples:DataFrame):DataFrame = {
    import samples.sparkSession.implicits._
    samples.map(row =>{
      (row.getAs[Int]("user_id"),
        row.getAs[Int]("item_id"),
        row.getAs[Int]("category_id"),
        row.getAs[String]("content_type"),
        row.getAs[String]("timestamp"),
        row.getAs[Long]("user_item_click"),
        row.getAs[Double]("user_item_imp"),
        row.getAs[Double]("item_ctr"),
        row.getAs[Int]("is_new_user"),
        Vectors.dense(row.getAs[Seq[Double]]("user_embedding").toArray),
        Vectors.dense(row.getAs[Seq[Double]]("item_embedding").toArray),
        row.getAs[Int]("label")
      )
    }).toDF("user_id","item_id","category_id","content_type","timestamp","user_item_click","user_item_imp","item_ctr","is_new_user","user_embedding","item_embedding","label")
  }

  /**
    * 计算user embedding和item embedding内积
    * @param samples
    * @return
    */
  def calculateEmbeddingInnerProduct(samples:DataFrame):DataFrame = {
    import samples.sparkSession.implicits._
    samples.map(row =>{
      val user_embedding = row.getAs[DenseVector]("user_embedding")
      val item_embedding = row.getAs[DenseVector]("item_embedding")
      var asquare = 0.0
      var bsquare = 0.0
      var abmul = 0.0

      for(i <-0 until user_embedding.size){
        asquare += user_embedding(i) * user_embedding(i)
        bsquare += item_embedding(i) * item_embedding(i)
        abmul += user_embedding(i) * item_embedding(i)
      }
      var inner_product = 0.0
      if(asquare == 0 || bsquare == 0){
        inner_product = 0.0
      }else{
        inner_product = abmul / (Math.sqrt(asquare) * Math.sqrt(bsquare))
      }

      (row.getAs[Int]("user_id"),
        row.getAs[Int]("item_id"),
        row.getAs[Int]("category_id"),
        row.getAs[String]("content_type"),
        row.getAs[String]("timestamp"),
        row.getAs[Long]("user_item_click"),
        row.getAs[Double]("user_item_imp"),
        row.getAs[Double]("item_ctr"),
        row.getAs[Int]("is_new_user"),
        inner_product,
        row.getAs[Int]("label")
      )
    }).toDF("user_id",
      "item_id",
      "category_id",
      "content_type",
      "timestamp",
      "user_item_click",
      "user_item_imp",
      "item_ctr",
      "is_new_user",
      "embedding_inner_product",
      "label"
    )
  }

  /**
    * 特征工程：字符串加索引，热独编码，分箱离散化，多列转化为单列向量，归一化
    * @param samples
    * @return
    */
  def preProcessInnerProductSamples(samples:DataFrame):PipelineModel = {
//    String类型->index(number)类型
    val contentTypeIndexer = new StringIndexer()
      .setInputCol("content_type")
      .setOutputCol("content_type_index")

    val oneHotEncode = new OneHotEncoderEstimator()
      .setInputCols(Array("content_type_index"))
      .setOutputCols(Array("content_type_vector"))
      .setDropLast(false)

//    分箱
    val ctr_discretizer = new QuantileDiscretizer()
      .setInputCol("item_ctr")
      .setOutputCol("ctr_bucket")
      .setNumBuckets(100)

//    多列转化为一列
    val vectorAsCols = Array("content_type_vector","ctr_bucket","user_item_click","user_item_imp","is_new_user","embedding_inner_product")
    val vectorAssembler = new VectorAssembler()
      .setInputCols(vectorAsCols)
      .setOutputCol("vectorFeature")

    val scaler = new MinMaxScaler()
      .setInputCol("vectorFeature")
      .setOutputCol("scaledFeatures")

    val pipelineStage:Array[PipelineStage] = Array(contentTypeIndexer, oneHotEncode, ctr_discretizer, vectorAssembler, scaler)
    val featurePipeline = new Pipeline().setStages(pipelineStage)

    featurePipeline.fit(samples)
  }

  //正常预处理样本以生成特征向量
  def preProcessSamples(samples:DataFrame):PipelineModel = {
    val contentTypeIndexer = new StringIndexer().setInputCol("content_type").setOutputCol("content_type_index")

    val oneHotEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array("content_type_index"))
      .setOutputCols(Array("content_type_vector"))
      .setDropLast(false)

    val ctr_discretizer = new QuantileDiscretizer()
      .setInputCol("item_ctr")
      .setOutputCol("ctr_bucket")
      .setNumBuckets(100)

    val vectorAsCols = Array("content_type_vector", "ctr_bucket", "user_item_click", "user_item_imp", "is_new_user", "user_embedding", "item_embedding")
    val vectorAssembler = new VectorAssembler().setInputCols(vectorAsCols).setOutputCol("vectorFeature")

    val scaler = new MinMaxScaler().setInputCol("vectorFeature").setOutputCol("scaledFeatures")

    /*
    val scaler = new StandardScaler()
      .setInputCol("vectorFeature")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(true)
    */

    val pipelineStage: Array[PipelineStage] = Array(contentTypeIndexer, oneHotEncoder, ctr_discretizer, vectorAssembler, scaler)
    val featurePipeline = new Pipeline().setStages(pipelineStage)

    featurePipeline.fit(samples)
  }
}
