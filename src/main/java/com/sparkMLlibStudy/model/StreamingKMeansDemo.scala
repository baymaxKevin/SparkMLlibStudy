package com.sparkMLlibStudy.model

import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-10-19 下午6:54
  * @Modified By:
  */
object StreamingKMeansDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamingKMeansDemo")
    val ssc = new StreamingContext(conf, Seconds(3))
    val lines = ssc.textFileStream("/opt/modules/spark-2.3.1/data/streaming_kmeans_data_test.txt")
    val trainData = lines.map(s => Vectors.dense(s.split(" ").map(_.toDouble)))
//    trainData.print()
    val numClusters = 3
    val numDimensions = 2
    val model = new StreamingKMeans().setK(numClusters).setDecayFactor(1.0).setRandomCenters(numDimensions, 0.0)
    model.trainOn(trainData)
    model.latestModel().clusterCenters.foreach(println)
//    model.predictOn(trainData).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
