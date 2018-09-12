package com.sparkMLlibStudy.model

import org.apache.spark.ml.clustering.{BisectingKMeans, GaussianMixture, KMeans, LDA}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-9-11 下午2:40
  * @Modified By:
  */
object ClusterModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ClusterModel")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

//    加载数据集
    val dataset = spark.read.format("libsvm").load("/opt/modules/spark-2.3.1/data/mllib/sample_kmeans_data.txt")

    /**
      * K-means(K均值聚类)
      */
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val kmModel = kmeans.fit(dataset)

//    预测
    val predictions = kmModel.transform(dataset)

//    K均值模型评估
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)
    println(s"Silhouette with squared euclidean distance = $silhouette")

    println("Cluster Centers: ")
    kmModel.clusterCenters.foreach(println)

//    加载数据
    val data = spark.read.format("libsvm")
      .load("/opt/modules/spark-2.3.1/data/mllib/sample_lda_libsvm_data.txt")

    /**
      * LDA模型
      */
    val lda = new LDA().setK(10).setMaxIter(10)
    val ldaModel = lda.fit(data)

    val ll = ldaModel.logLikelihood(data)
    val lp = ldaModel.logPerplexity(data)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

//    描述topics
    val topics = ldaModel.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    val transformed = ldaModel.transform(dataset)
    transformed.show(false)

    /**
      * Bisecting k-means
      */
    val bkm = new BisectingKMeans().setK(2).setSeed(1)
    val bkmModel = bkm.fit(dataset)

    // Evaluate clustering.
    val cost = bkmModel.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $cost")

    println("Cluster Centers: ")
    val centers = bkmModel.clusterCenters
    centers.foreach(println)

    /**
      * Gaussian Mixture Model (GMM)
      */
    val gmm = new GaussianMixture()
      .setK(2)
    val gmmModel = gmm.fit(dataset)

    for (i <- 0 until gmmModel.getK) {
      println(s"Gaussian $i:\nweight=${gmmModel.weights(i)}\n" +
        s"mu=${gmmModel.gaussians(i).mean}\nsigma=\n${gmmModel.gaussians(i).cov}\n")
    }
  }
}
