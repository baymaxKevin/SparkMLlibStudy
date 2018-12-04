package com.sparkMLlibStudy.evaluation

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.DataFrame

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-12-4 下午6:25
  * @Modified By:
  */
class Evaluator {
  def evaluate(predictions:DataFrame):Unit = {

    import  predictions.sparkSession.implicits._

    val scoreAndLabels = predictions.select("label", "probability").map { row =>
      (row.apply(1).asInstanceOf[DenseVector](1), row.getAs[Int]("label").toDouble)
    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels.rdd)

    println("AUC under PR = " + metrics.areaUnderPR())
    println("AUC under ROC = " + metrics.areaUnderROC())
  }

}
