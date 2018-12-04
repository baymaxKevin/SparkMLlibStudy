package com.sparkMLlibStudy.ctrModel

import com.sparkMLlibStudy.features.FeatureEngineering
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.DataFrame

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-12-4 下午6:13
  * @Modified By:
  */
class RandomForestCtrModel {
  var _pipelineModel:PipelineModel = _
  var _model:RandomForestClassificationModel = _

  def train(samples:DataFrame) : Unit = {
    _pipelineModel = new FeatureEngineering().preProcessSamples(samples)

    _model = new RandomForestClassifier()
      .setNumTrees(10)    //树的数量
      .setMaxDepth(4)     //每棵树最大深度
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("label")
      .fit(_pipelineModel.transform(samples))
  }

  def transform(samples:DataFrame):DataFrame = {
    _model.transform(_pipelineModel.transform(samples))
  }
}

