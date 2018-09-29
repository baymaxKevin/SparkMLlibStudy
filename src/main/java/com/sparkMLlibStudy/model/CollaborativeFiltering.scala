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

  final private val PRIOR_COUNT = 10
  final private val PRIOR_CORRELATION = 0

  final private val COOCWEIGHT = 0.1
  final private val CORWEIGHT = 0.1
  final private val REGCORWEIGHT = 0.1
  final private val COSWEIGHT = 0.2
  final private val IMPCOSWEIGHT = 0.3
  final private val JACWEIGHT = 0.2

  final private val RANKS = 10

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ClusterModel")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val path = "/opt/modules/spark-2.3.1/data/mllib/als/sample_movielens_ratings.txt"
    import spark.implicits._
    val ratings = spark.read.textFile(path).map(parseRating(_)).toDF()

    val coef = Array(COOCWEIGHT,CORWEIGHT,REGCORWEIGHT,COSWEIGHT,IMPCOSWEIGHT,JACWEIGHT)
    CollaborativeFilterItemBased(path, spark, 2, coef)
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
    * 用户预测评分
    * 选取用户预测评分最高的top10作为推荐结果
    * @param path 数据加载路径
    * @param spark sparkSession设置
    * @param defaultParallelism 分区
    * @param coef 共现相似度,皮尔逊相关系数,改进皮尔逊相关系数,余弦相似度,改进余弦相似度,Jaccard距离对应的加权系数
    */
  def CollaborativeFilterItemBased(path:String, spark:SparkSession, defaultParallelism:Int, coef:Array[Double]) = {
    import spark.implicits._
    val rating = spark.read.textFile(path).map(parseRating(_)).toDF()
    rating.show(10, false)
//    每个用户评分最高的top10物品
    val userRecs = rating.select($"userId", $"movieId", $"rating", functions.row_number().over(Window.partitionBy("userId").orderBy("rating")).alias("rank"))
      .filter($"rank" <= 10)
    userRecs.show(10, false)
//    获取每个物品评分用户数,item2manyUser格式如下(movieId,numRaters)
//    rating.groupBy($"movieId").pivot("rating").count().show(false)
    val item2manyUser = rating.groupBy($"movieId").count().toDF("movieId", "numRaters").coalesce(defaultParallelism)
    item2manyUser.show(10, false)
//    获取用户对物品评分及评分物品数量,ratingWithSize和ratingWithSizePair格式如下(userId,movieId,rating,timestamp,numRaters)
    val ratingsWithSize = rating.join(item2manyUser, "movieId").coalesce(defaultParallelism)
    ratingsWithSize.show(10, false)
    val ratingsWithSizePair = ratingsWithSize.toDF("movieIdPair", "userId", "ratingPair", "timestampPair", "numRatersPair")
//    获取用户对不同物品的评分的矩阵，并过滤相同item pairs
    val ratingPairs = ratingsWithSize.join(ratingsWithSizePair, "userId").where($"movieId" < $"movieIdPair")
      .selectExpr("userId", "movieId", "rating", "numRaters", "movieIdPair", "ratingPair", "numRatersPair", "rating * ratingPair as product", "pow(rating,2) as ratingPow", "pow(ratingPair,2) as ratingPairPow")
      .coalesce(defaultParallelism)
    ratingPairs.show(10, false)
//    计算item pairs的相似度统计量
    val vectorCals = ratingPairs.groupBy("movieId", "movieIdPair")
      .agg(functions.count("userId").alias("size"),
        functions.sum("product").alias("dotProduct"),
        functions.sum("rating").alias("ratingSum"),
        functions.sum("ratingPair").alias("ratingPairSum"),
        functions.sum("ratingPow").alias("ratingPowSum"),
        functions.sum("ratingPairPow").alias("ratingPairPowSum"),
        functions.first("numRaters").alias("numRaters"),
        functions.first("numRatersPair").alias("numRatersPair"))
//      .agg(Map("userId"->"count","product"->"sum","rating"->"sum","ratingPair"->"sum","ratingPow"->"sum","ratingPairPow"->"sum","numRaters"->"first","numRatersPair"->"first"))
//      .toDF("movieId","movieIdPair","size","dotProduct","ratingSum","ratingPairSum","ratingPowSum","ratingPairPowSum","numRaters","numRatersPair")
      .coalesce(defaultParallelism)
    vectorCals.show(10, false)
//    计算item pairs的相似度度量(包括:共现相似度、改进共现相似度、皮尔逊系数、改进皮尔逊系数、余弦相似度、改进余弦相似度、Jaccard距离)
    val similar = vectorCals.map(row => {
      val movieId = row.getAs[Int]("movieId")
      val movieIdPair = row.getAs[Int]("movieIdPair")
      val size = row.getAs[Long]("size")
      val dotProduct = row.getAs[Double]("dotProduct")
      val ratingSum = row.getAs[Double]("ratingSum")
      val ratingPairSum = row.getAs[Double]("ratingPairSum")
      val ratingPowSum = row.getAs[Double]("ratingPowSum")
      val ratingPairPowSum = row.getAs[Double]("ratingPairPowSum")
      val numRaters = row.getAs[Long]("numRaters")
      val numRatersPair = row.getAs[Long]("numRatersPair")

      val cooc = cooccurrence(size, numRaters, numRatersPair)
      val corr = correlation(size, dotProduct, ratingSum, ratingPairSum, ratingPowSum, ratingPairPowSum)
      val regCorr = regularCorrelation(size, dotProduct, ratingSum, ratingPairSum, ratingPowSum, ratingPairPowSum, PRIOR_COUNT, PRIOR_CORRELATION)
      val cos = cosineSimilarity(dotProduct, math.sqrt(ratingPowSum), math.sqrt(ratingPairPowSum))
      val impCos = improvedCosineSimilarity(dotProduct, math.sqrt(ratingPowSum), math.sqrt(ratingPairPowSum), size, numRaters, numRatersPair)
      val jac = jaccardSimilarity(size, numRaters, numRatersPair)
      val score = coef(0)*cooc + coef(1)*corr + coef(2)*regCorr - coef(3)*cos - coef(4)*impCos + coef(5)*jac
      (movieId, movieIdPair, cooc, corr, regCorr, cos, impCos, jac, score)
    }).toDF("movieId", "movieIdPair", "cooc", "corr", "regCorr", "cos", "impCos", "jac", "score")
    similar.show(10, false)
//    半角矩阵反转，计算所有item pairs相似度度量
    val similarities = similar.withColumnRenamed("movieId", "movieIdRe")
      .withColumnRenamed("movieIdPair", "movieId")
      .withColumnRenamed("movieIdRe","movieIdPair")
      .union(similar)
      .repartition(defaultParallelism)
    similarities.show(10, false)
    val ItemPairSim = similarities.groupBy("movieId","movieIdPair").agg(
      functions.sum("cooc").alias("coocSim"),
      functions.sum("corr").alias("corrSim"),
      functions.sum("regCorr").alias("regCorrSim"),
      functions.sum("cos").alias("cosSim"),
      functions.sum("impCos").alias("impCosSim"),
      functions.sum("jac").alias("jacSim"),
      functions.sum("score").alias("scores")
    )
    val simCols = Array("coocSim","corrSim","regCorrSim","cosSim","impCosSim","jacSim","scores")
    simCols.map(simCol =>{
      simCol match{
        case "coocSim" => println("共现相似度:")
        case "corrSim" => println("皮尔逊相关系数:")
        case "regCorrSim" => println("改进皮尔逊相关系数:")
        case "cosSim" => println("余弦相似度:")
        case "impCosSim" => println("改进的余弦相似度:")
        case "jacSim" => println("Jaccard相似度:")
        case _ => println("加权相似度:")
      }
      val itemPairsCol = if(simCol.equals("cosSim")||simCol.equals("impCosSim")){
        ItemPairSim.select( $"movieId", $"movieIdPair", functions.row_number().over(Window.partitionBy("movieId").orderBy(simCol)).alias("rank"))
          .filter($"rank" <= 10)
      }else{
        ItemPairSim.select( $"movieId", $"movieIdPair", functions.row_number().over(Window.partitionBy("movieId").orderBy(functions.desc(simCol))).alias("rank"))
          .filter($"rank" <= 10)
      }
      println(itemPairsCol.select("movieId", "movieIdPair").where($"movieId" === 15).collectAsList())
    })
    ItemPairSim.where($"movieId" === 15).show(10,false)
//    用户评分与item pairs连接
    val userRating = rating.join(similar, "movieId")
      .selectExpr("userId", "movieId", "movieIdPair", "cooc", "cooc * rating as coocMeasure",
        "corr", "corr * rating as corrMeasure", "regCorr", "regCorr * rating as regCorrMeasure",
        "cos", "cos * rating as cosMeasure", "impCos", "impCos * rating as impCosMeasure", "jac", "jac * rating as jacMeasure", "score", "score * rating as scoreMeasure")
      .coalesce(defaultParallelism)
    userRating.show(10, false)
//    用户对所有物品评分预测
    val userScore = userRating.groupBy("userId", "movieIdPair")
      .agg(functions.sum("cooc").alias("coocSum"),
        functions.sum("coocMeasure").alias("coocMeasureSum"),
        functions.sum("corr").alias("corrSum"),
        functions.sum("corrMeasure").alias("corrMeasureSum"),
        functions.sum("regCorr").alias("regCorrSum"),
        functions.sum("regCorrMeasure").alias("regCorrMeasureSum"),
        functions.sum("cos").alias("cosSum"),
        functions.sum("cosMeasure").alias("cosMeasureSum"),
        functions.sum("impCos").alias("impCosSum"),
        functions.sum("impCosMeasure").alias("impCosMeasureSum"),
        functions.sum("jac").alias("jacSum"),
        functions.sum("jacMeasure").alias("jacMeasureSum"),
        functions.sum("score").alias("score"),
        functions.sum("scoreMeasure").alias("scoreMeasure")
      )
      .selectExpr("userId", "movieIdPair", "coocSum/coocMeasureSum as coocScore",
        "corrSum/corrMeasureSum as corrScore", "regCorrSum/regCorrMeasureSum as regCorrScore",
        "cosSum/cosMeasureSum as cosScore", "impCosSum/impCosMeasureSum as impCosScore",
        "jacSum/jacMeasureSum as jacScore","score/scoreMeasure as scores")
      .coalesce(defaultParallelism)
//    选取每个用户评分最高的10个商品
    val userRanks = userScore
      .select($"userId", $"movieIdPair", $"scores", functions.row_number().over(Window.partitionBy("userId").orderBy(functions.desc("scores"))).alias("rank"))
      .filter($"rank" <= RANKS)
    val userRecommend = userRanks.select($"userId", functions.concat_ws(":", $"movieIdPair", $"scores").alias("recommend"))
      .groupBy("userId")
      .agg(functions.collect_set("recommend"))
    userRecommend.show(10, false)
  }

  /**
    * 改进的共现相似度
    * 共现相似度=numRatersPairs/numRaterPair，同时对A和B感兴趣的用户数/对A感兴趣的用户数
    * 描述喜欢A(numRaters)的用户有多大概率对B(numRatersPair)感兴趣，但B是热门物品，导致共现相似度为1
    * 改进的共现相似度=numRatersPairs/sqrt(numRaters * numRatersPair)，惩罚物品B权重，减轻热门物品和很多相似物品的可能性
    * @param numRatersPairs
    * @param numRaters
    * @param numRatersPair
    * @return
    */
  def cooccurrence(numRatersPairs:Long,numRaters:Long,numRatersPair:Long):Double = {
    numRatersPairs / math.sqrt(numRaters * numRatersPair)
  }

  /**
    * 皮尔逊相关系数=变量协方差/标准差
    * @param size
    * @param dotProduct
    * @param ratingSum
    * @param ratingPairSum
    * @param ratingNorm
    * @param ratingNormPair
    * @return
    */
  def correlation(size:Double,dotProduct:Double,ratingSum:Double,ratingPairSum:Double,ratingNorm:Double,ratingNormPair:Double):Double = {
    val numerator = size * dotProduct - ratingSum * ratingPairSum
    val denomiator = math.sqrt(size * ratingNorm - ratingSum * ratingSum) * math.sqrt(size * ratingNormPair - ratingPairSum * ratingPairSum)
    if(denomiator==0) 0 else numerator/denomiator
  }

  /**
    * 正则化相关系数
    * @param size
    * @param dotProduct
    * @param ratingSum
    * @param ratingPairSum
    * @param ratingNorm
    * @param ratingNormPair
    * @param virtualCount
    * @param priorCorrelation
    * @return
    */
  def regularCorrelation(size:Double,dotProduct:Double,ratingSum:Double,ratingPairSum:Double,ratingNorm:Double,ratingNormPair:Double,virtualCount:Double,priorCorrelation:Double):Double = {
    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, ratingPairSum, ratingNorm, ratingNormPair)
    val w = size/(size + virtualCount)
    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }

  /**
    * 余弦相似度
    * @param dotProduct
    * @param ratingNorm
    * @param ratingNormPair
    * @return
    */
  def cosineSimilarity(dotProduct:Double, ratingNorm:Double, ratingNormPair:Double):Double = {
    dotProduct/(ratingNorm * ratingNormPair)
  }

  /**
    * 改进的余弦相似度
    * 考虑两个向量相同个体个数，A向量大小和B向量大小
    * @param dotProduct
    * @param ratingNorm
    * @param ratingNormPair
    * @param numPairs
    * @param num
    * @param numPair
    * @return
    */
  def improvedCosineSimilarity(dotProduct:Double,ratingNorm:Double,ratingNormPair:Double,numPairs:Long,num:Long,numPair:Long):Double = {
    dotProduct * numPairs / (ratingNorm * ratingNormPair * num * math.log10(10 + numPair))
  }

  /**
    * Jaccard相似度
    * @param size
    * @param numRaters
    * @param numRatersPair
    * @return
    */
  def jaccardSimilarity(size:Double,numRaters:Double,numRatersPair:Double):Double = {
    size/(numRaters + numRatersPair - size)
  }
}
