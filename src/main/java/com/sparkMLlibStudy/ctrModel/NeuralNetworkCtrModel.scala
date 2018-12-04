package com.sparkMLlibStudy.ctrModel

import com.sparkMLlibStudy.features.FeatureEngineering
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.DataFrame

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-12-4 下午2:06
  * @Modified By:
  */
class NeuralNetworkCtrModel {
  var _pipelineModel:PipelineModel = _
  var _model:MultilayerPerceptronClassificationModel = _

  def train(samples:DataFrame) : Unit = {
    _pipelineModel = new FeatureEngineering().preProcessSamples(samples)

    val preparedSamples = _pipelineModel.transform(samples)

    //network architecture, better to keep tuning it until metrics converge
    val layers = Array[Int](preparedSamples.first().getAs[DenseVector]("scaledFeatures").toArray.length,
      preparedSamples.first().getAs[DenseVector]("scaledFeatures").toArray.length / 2, 2)

    val nnModel = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(150)                //最大迭代次数，如果损失函数或指标不收敛，则继续增加迭代次数
      .setStepSize(0.005)             //学习步长越大，步长越大会导致振动损失
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("label")

    _model = nnModel.fit(preparedSamples)
  }

  def transform(samples:DataFrame):DataFrame = {
    _model.transform(_pipelineModel.transform(samples))
  }
}
