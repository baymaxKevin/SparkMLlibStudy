package com.sparkMLlibStudy.model

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-9-12 下午6:00
  * @Modified By:
  */
object CollaborativeFiltering {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ClusterModel")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._
    val ratings = spark.read.textFile("/opt/modules/spark-2.3.1/data/mllib/als/sample_movielens_ratings.txt")
      .map(parseRating(_))
      .toDF()

    CollaborativeFilterItemBased("/opt/modules/spark-2.3.1/data/mllib/als/sample_movielens_ratings.txt", spark)
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    /**
      * ALS(交替最小二乘法)
      */
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)


    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

//    为每个用户生成前10个电影推荐
    val userRecs = model.recommendForAllUsers(10)
    userRecs.show(10, false)
//    为每部电影生成前10个用户推荐
    val movieRecs = model.recommendForAllItems(10)
    movieRecs.show(10,false)

//    为指定的一组用户生成前10个电影推荐
    val users = ratings.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users, 10)
    userSubsetRecs.show(10, false)

//    为指定的一组电影生成前10个用户推荐
    val movies = ratings.select(als.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    movieSubSetRecs.show(5, false)
  }
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  /**
    * CF item-based
    */
  def CollaborativeFilterItemBased(path:String, spark:SparkSession) = {
    import spark.implicits._
    val rating = spark.read.textFile(path).map(parseRating(_)).toDF()
//    每个用户评分最高的top10物品
    val userRecs = rating.select($"userId",$"movieId",$"rating",functions.row_number().over(Window.partitionBy("userId").orderBy("rating")).alias("rank"))
      .filter($"rank" <= 10)
//    获取每个物品用户评分,item2manyUser格式如下(userId,itemId,rating,numRaters,timestamp)
//    rating.groupBy($"movieId").pivot("rating").count().show(false)
    val item2manyUser = rating.groupBy($"movieId").count().alias("numRaters")
    val ratingsWithSize = rating.join(item2manyUser,"movieId")
//    ratingWithSize自join,过滤重复的item pairs减少运算量
    val ratingPairs = ratingsWithSize.join(ratingsWithSize,"userId").select("userId","movieId","rating","numRaters")
    ratingPairs.show(100,false)
  }
}
