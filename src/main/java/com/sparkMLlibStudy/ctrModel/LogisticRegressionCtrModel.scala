package com.sparkMLlibStudy.ctrModel

import com.sparkMLlibStudy.features.FeatureEngineering
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.DataFrame

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-12-4 下午2:02
  * @Modified By:
  */
class LogisticRegressionCtrModel {
  var _pipelineModel:PipelineModel = _
  var _model:LogisticRegressionModel = _

  def train(samples:DataFrame) : Unit = {
    _pipelineModel = new FeatureEngineering().preProcessSamples(samples)

    _model = new LogisticRegression()
      .setMaxIter(20)           //最大迭代次数
      .setRegParam(0.0)         //正则化参数
      .setElasticNetParam(0.0)  //0-L2正则 1-L1正则
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("label")
      .fit(_pipelineModel.transform(samples))
  }

  def transform(samples:DataFrame):DataFrame = {
    _model.transform(_pipelineModel.transform(samples))
  }
}
