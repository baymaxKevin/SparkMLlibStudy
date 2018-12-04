package com.sparkMLlibStudy.ctrModel

import com.sparkMLlibStudy.features.FeatureEngineering
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.linalg.DenseVector

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-12-4 上午11:34
  * @Modified By:
  */
class InnerProductNNCtrModel {
  var _pipelineModel:PipelineModel = _
  var _model:MultilayerPerceptronClassificationModel = _

  def train(samples:DataFrame) : Unit = {

    val fe = new FeatureEngineering()

    //计算用户和item内积
    val samplesWithInnerProduct = fe.calculateEmbeddingInnerProduct(samples)
    _pipelineModel = fe.preProcessInnerProductSamples(samplesWithInnerProduct)

    val preparedSamples = _pipelineModel.transform(samplesWithInnerProduct)

    //网络架构，最好继续调整它，直到目标收敛
    val layers = Array[Int](preparedSamples.first().getAs[DenseVector]("scaledFeatures").toArray.length,
      preparedSamples.first().getAs[DenseVector]("scaledFeatures").toArray.length / 2, 2)

    val nnModel = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(150)      //最大迭代次数，如果损失函数或指标不收敛，则继续增加迭代次数
      .setStepSize(0.005)   //学习步长越大，步长越大会导致振动损失
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("label")

    _model = nnModel.fit(preparedSamples)
  }

  def transform(samples:DataFrame):DataFrame = {
    val samplesWithInnerProduct = new FeatureEngineering().calculateEmbeddingInnerProduct(samples)
    _model.transform(_pipelineModel.transform(samplesWithInnerProduct))
  }
}
