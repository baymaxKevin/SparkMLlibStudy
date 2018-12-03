package org.apache.spark.ml.gbtlr

import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.SparkException
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.mllib.linalg.{DenseVector => OldDenseVector}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.{FeatureType, Algo => OldAlgo, BoostingStrategy => OldBoostingStrategy, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impurity.{Variance => OldVariance}
import org.apache.spark.mllib.tree.loss.{LogLoss => OldLogLoss, Loss => OldLoss}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, GradientBoostedTreesModel, Node => OldNode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-11-27 下午5:39
  * @Modified By:
  */

trait GBTLRClassifierParams extends Params {

  // =====GBDT分类器参数设置=====
  /**
    * 设置检查点间隔参数(>= 1) 或 禁用检查点(-1).
    *
    * 例如 设置为10 则每10次缓存启用检查点
    * @group param
    */
  val checkpointInterval: IntParam = new IntParam(this, "checkpointInterval", "设置检查点间隔参数(>= 1) 或 禁用检查点(-1) 例如 设置为10 则每10次缓存启用检查点",
    (interval: Int) => interval == -1 || interval >= 1)

  /** @group getParam */
  def getCheckpointInterval: Int = $(checkpointInterval)

  /**
    * GBDT最小化构造损失函数(不区分大小写)
    *
    * 支持: "logistic, squared, absolute", 默认logistic
    * @group param
    */
  val lossType: Param[String] = new Param[String](this, "lossType", "GBDT最小化构造损失函数(不区分大小写). 支持类型: logistic, squared, absolute",
    (value: String) => value == "logistic")

  /** @group getParam */
  def getLossType: String = $(lossType).toLowerCase

  /**
    * 每个节点用于离散连续特征和选择拆分最大分箱数
    * 分箱越多，粒度越高
    * 必须>= 2且>=任何分类特征中的类别数
    *
    * (default = 32)
    * @group param
    */
  val maxBins: IntParam = new IntParam(this, "maxBins", "每个节点用于离散连续特征和选择拆分最大分箱数. 必须>= 2且>=任何分类特征中的类别数.", ParamValidators.gtEq(2))

  /** @group getParam */
  def getMaxBins: Int = $(maxBins)

  /**
    * 树最大深度 ( >= 0).
    * 深度0意味着1个叶节点; 深度1意味着1个中间节点，2个叶节点
    *
    * (default = 5)
    * @group param
    */
  val maxDepth: IntParam =
    new IntParam(this, "maxDepth", "树最大深度 ( >= 0)." +
      " 深度0意味着1个叶节点; 深度1意味着1个中间节点，2个叶节点.",
      ParamValidators.gtEq(0))

  /** @group getParam */
  def getMaxDepth: Int = $(maxDepth)

  /**
    * 如果为false，算法将树传递给执行程序以匹配具有节点的实例
    *
    * 如果为true，则算法将缓存每个实例的节点ID
    *
    * 缓存可以加快深层树木的训练, 用户可以设置多长时间
    * 通过设置checkpointInterval来对缓存进行检查点设置或禁用它
    *
    * (default = false)
    * @group param
    */
  val cacheNodeIds: BooleanParam = new BooleanParam(this, "cacheNodeIds", "如果为false，算法将树传递给执行程序以匹配具有节点的实例." +
    "如果为true，则算法将缓存每个实例的节点ID. 缓存可以加快深层树木的训练, 用户可以设置多长时间.")

  /** @group getParam */
  def getCacheNodeIds: Boolean = $(cacheNodeIds)

  /**
    * 分配给直方图聚合的最大内存（MB）.
    * 如果太小，然后每次迭代将拆分1个节点，其聚合可能超过此大小.
    *
    * (default = 256 MB)
    *
    * @group param
    */
  val maxMemoryInMB: IntParam = new IntParam(this, "maxMemoryInMB",
    "分配给直方图聚合的最大内存（MB）.",
    ParamValidators.gtEq(0))

  /** @group getParam */
  def getMaxMemoryInMB: Int = $(maxMemoryInMB)

  /**
    * 分割后每个叶节点必须具有的最小实例数.
    * 如果分裂导致左或右叶节点的minInstancesPerNode少于minInstancesPerNode，拆分将被视为无效。
    * 应该 >= 1.
    *
    * (default = 1)
    *
    * @group param
    */
  val minInstancesPerNode: IntParam = new IntParam(this, "minInstancesPerNode",
    "分割后每个叶节点必须具有的最小实例数. " +
      "如果分裂导致左或右叶节点的minInstancesPerNode少于minInstancesPerNode，拆分将被视为无效。 应该 >= 1.", ParamValidators.gtEq(1))

  /** @group getParam */
  def getMinInstancePerNode: Int = $(minInstancesPerNode)

  /**
    * 在树节点处考虑拆分的最小信息增益
    * 应该 >= 0.0.
    *
    * (default = 0.0)
    *
    * @group param
    */
  val minInfoGain: DoubleParam = new DoubleParam(this, "minInfoGain",
    "在树节点处考虑拆分的最小信息增益。",
    ParamValidators.gtEq(0.0))

  /** @group getParam */
  def getMinInfoGain: Double = $(minInfoGain)

  /**
    * 用于GBDT的最大迭代次数（>= 0）的参数
    * @group param
    */
  val GBTMaxIter: IntParam = new IntParam(this, "GBTMaxIter",
    "用于GBDT的最大迭代次数（>= 0）的参数",
    ParamValidators.gtEq(0))

  /** @group getParam */
  def getGBTMaxIter: Int = $(GBTMaxIter)

  /**
    * 区间(0,1]的步长(即学习率)的参数，用于缩小每个估计量贡献
    *
    * (default = 0.1)
    *
    * @group param
    */
  val stepSize: DoubleParam = new DoubleParam(this, "stepSize", "区间(0,1]的步长(即学习率)的参数，用于缩小每个估计量贡献.",
    ParamValidators.inRange(0, 1, lowerInclusive = false, upperInclusive = true))

  /** @group getParam */
  def getStepSize: Double = $(stepSize)

  /**
    * 用于学习每个决策树的训练数据的分数，范围（0,1]
    *
    * (default = 1.0)
    * @group param
    */
  val subsamplingRate: DoubleParam = new DoubleParam(this, "subsamplingRate",
    "用于学习每个决策树的训练数据的分数，范围(0,1].",
    ParamValidators.inRange(0, 1, lowerInclusive = false, upperInclusive = true))

  /** @group getParam */
  def getSubsamplingRate: Double = $(subsamplingRate)

  /**
    * 随机种子的参数
    * @group param
    */
  val seed: LongParam = new LongParam(this, "seed", "random seed")

  /** @group getParam */
  def getSeed: Long = $(seed)

  // =====below are LR params=====

  /**
    * ElasticNet混合参数的参数，范围为[0,1]
    * 对于alpha = 0，是L2正则
    * 对于alpha = 1，是L1正则
    * @group param
    */
  val elasticNetParam: DoubleParam = new DoubleParam(this, "elasticNetParam",
    "ElasticNet混合参数的参数，范围为[0,1]. 对于alpha = 0，是L2正则. 对于alpha = 1，是L1正则", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getElasticNetParam: Double = $(elasticNetParam)

  /**
    * 名称族的参数，是对模型中使用的标签分布的描述
    *
    * 支持的选项:
    *
    *  - "auto": 根据分类数自动选择，如果numClasses == 1 || numClasses == 2，设置为“二项式”
    *      否则，设置为“多项”
    *
    *  - "binomial": 二分类逻辑回归
    *
    *  - "multinomial": 多分类逻辑回归
    *
    * Default is "auto".
    *
    * @group param
    */
  val family: Param[String] = new Param(this, "family",
    "名称族的参数，是对模型中使用的标签分布的描述. 支持选项: " +
      s"${Array("auto", "binomial", "multinomial").map(_.toLowerCase).mkString(", ")}.",
    ParamValidators.inArray[String](
      Array("auto", "binomial", "multinomial").map(_.toLowerCase)))

  /** @group getParam */
  def getFamily: String = $(family)

  /**
    * 是否适合截取项的参数
    * @group param
    */
  val fitIntercept: BooleanParam = new BooleanParam(this, "fitIntercept",
    "是否适合截取项")

  /** @group getParam */
  def getFitIntercept: Boolean = $(fitIntercept)

  /**
    * 最大迭代次数(>=0)的参数
    * @group param
    */
  val LRMaxIter: IntParam = new IntParam(this, "LRMaxIter",
    "最大迭代次数(>=0)的参数",
    ParamValidators.gtEq(0))

  /** @group getParam */
  def getLRMaxIter: Int = $(LRMaxIter)

  /**
    * 用于预测类条件概率的列名的参数.
    *
    * 注意: 并非所有模型都输出经过良好校准的概率估算!
    *
    * 这些概率应视为置信度，而非精确概率.
    *
    * @group param
    */
  val probabilityCol: Param[String] = new Param[String](this, "probabilityCol",
    "用于预测类条件概率的列名的参数. 注意: 并非所有模型都输出经过良好校准的概率估算!" +
      " 这些概率应视为置信度，而非精确概率.")

  /** @group getParam */
  def getProbabilityCol: String = $(probabilityCol)

  /**
    * 用于原始预测(即置信度)列名的参数
    *
    * @group param
    */
  val rawPredictionCol: Param[String] = new Param[String](this, "rawPredictionCol",
    "用于原始预测(即置信度)列名的参数")

  /** @group getParam */
  def getRawPredictionCol: String = $(rawPredictionCol)

  /**
    * GBDT生成特征列名的参数
    *
    * @group param
    */
  val gbtGeneratedFeaturesCol: Param[String] = new Param[String](this, "gbtGeneratedCol",
    "GBDT生成特征列名的参数")

  /** @group getParam */
  def getGbtGeneratedFeaturesCol: String = $(gbtGeneratedFeaturesCol)

  /**
    * 正则化参数 (>= 0)
    * @group param
    */
  val regParam: DoubleParam = new DoubleParam(this, "regParam",
    "正则化参数 (>= 0)", ParamValidators.gtEq(0))

  /** @group getParam */
  def getRegParam: Double = $(regParam)

  /**
    * 是否在拟合模型之前标准化训练特征的参数
    * @group param
    */
  val standardization: BooleanParam = new BooleanParam(this, "standardization",
    "是否在拟合模型之前标准化训练特征的参数")

  /** @group getParam */
  def getStandardization: Boolean = $(standardization)

  /**
    * 二项分类预测中的阈值参数，范围为[0,1]
    * @group param
    */
  val threshold: DoubleParam = new DoubleParam(this, "threshold",
    "二项分类预测中的阈值参数，范围为[0,1]",
    ParamValidators.inRange(0, 1))

  /**
    * 获取二分类阈值
    *
    * 如果thresholds设置长度为2（即二分类）,
    * 返回等效阈值：{{{1 /(1 + thresholds(0)/ thresholds(1))}}}
    * 否则，如果设置则返回threshold，如果未设置则返回其默认值
    *
    * @group getParam
    * @throws IllegalArgumentException if `thresholds` is set to an array of length other than 2.
    */
  def getThreshold: Double = {
    checkThresholdConsistency()
    if (isSet(thresholds)) {
      val ts = $(thresholds)
      require(ts.length == 2, "逻辑回归 getThreshold方法仅适用于二分类, 但是阈值长度不等于2.  阈值是: " +
        ts.mkString(","))
      1.0 / (1.0 + ts(0) / ts(1))
    } else {
      $(threshold)
    }
  }

  /**
    * 在多类分类中调整预测每个类的概率阈值参数
    * 数组的长度必须等于类的数量
    * 值> 0除外，最多一个值可能为0
    * 预测具有最大值p/t的类，其中p是该类的原始概率，t是类的阈值
    * @group param
    */
  val thresholds: DoubleArrayParam = new DoubleArrayParam(this, "thresholds",
    "在多类分类中调整预测每个类的概率阈值参数. 数组的长度必须等于类的数量. 预测具有最大值p/t的类，其中p是该类的原始概率，t是类的阈值。", (t: Array[Double]) => t.forall(_ >= 0) && t.count(_ == 0) <= 1)

  /**
    * 获取二进制或多类分类的阈值
    *
    * 如果设置了“thresholds”，则返回其值
    * 否则，如果设置了“threshold”，则返回二进制的等效阈值
    * 分类: (1-threshold, threshold).
    * 如果两者均未设置，则抛出异常
    *
    * @group getParam
    */
  def getThresholds: Array[Double] = {
    checkThresholdConsistency()
    if (!isSet(thresholds) && isSet(threshold)) {
      val t = $(threshold)
      Array(1-t, t)
    } else {
      $(thresholds)
    }
  }

  /**
    * 用于迭代算法的收敛容差的参数(>=0)
    * @group param
    */
  val tol: DoubleParam = new DoubleParam(this, "tol",
    "用于迭代算法的收敛容差的参数(>=0)", ParamValidators.gtEq(0))

  /** @group getParam */
  def getTol: Double = $(tol)

  /**
    * 权重列名的参数。如果未设置或为空，则将所有实例权重置为1.0。
    * @group param
    */
  val weightCol: Param[String] = new Param[String](this, "weightCol",
    "权重列名的参数。如果未设置或为空，则将所有实例权重置为1.0")

  /** @group getParam */
  def getWeightCol: String = $(weightCol)

  /**
    * 树的建议深度的参数(>= 2)
    * @group expertParam
    */
  val aggregationDepth: IntParam = new IntParam(this, "aggregationDepth",
    "树的建议深度的参数(>= 2)", ParamValidators.gtEq(2))

  /** @group expertGetParam */
  def getAggregationDepth: Int = $(aggregationDepth)

  /**
    * 如果同时设置了“threshold”和“thresholds”，则确保它们是一致的。
    *
    * @throws IllegalArgumentException if `threshold` and `thresholds` are not equivalent
    */
  private def checkThresholdConsistency(): Unit = {
    if (isSet(threshold) && isSet(thresholds)) {
      val ts = $(thresholds)
      require(ts.length == 2, "逻辑回归发现常量threshold和" +
        s" thresholds.  threshold值为 (${$(threshold)}), 表示二分类," +
        s" 但是thresholds长度是${ts.length}.清除一个参数以解决此问题.")
      val t = 1.0 / (1.0 + ts(0) / ts(1))
      require(math.abs($(threshold) - t) < 1E-5, "逻辑回归getThreshold发现" +
        s" threshold值为 (${$(threshold)})，thresholds (等于$t)")
    }
  }

  setDefault(seed -> this.getClass.getName.hashCode.toLong,
    subsamplingRate -> 1.0, GBTMaxIter -> 20, stepSize -> 0.1, maxDepth -> 5, maxBins -> 32,
    minInstancesPerNode -> 1, minInfoGain -> 0.0, checkpointInterval -> 10, fitIntercept -> true,
    probabilityCol -> "probability", rawPredictionCol -> "rawPrediction", standardization -> true,
    threshold -> 0.5, lossType -> "logistic", cacheNodeIds -> false, maxMemoryInMB -> 256,
    regParam -> 0.0, elasticNetParam -> 0.0, family -> "auto", LRMaxIter -> 100, tol -> 1E-6,
    aggregationDepth -> 2, gbtGeneratedFeaturesCol -> "gbt_generated_features")
}


/**
  * GBTLRClassifier是Gradient Boosting Trees和Logistic回归的混合模型
  * 输入功能通过增强的决策树进行转换. 每个树的输出被视为稀疏线性分类器的分类输入特征
  * 提升的决策树被证明是非常强大的特征变换
  *
  * 有关GBTLR的模型详细信息，请参见以下文章:
  * <a href="https://dl.acm.org/citation.cfm?id=2648589">Practical Lessons from Predicting Clicks on Ads at Facebook</a>
  *
  * Spark上的GBTLRClassifier是通过在Spark MLlib中结合使用GradientBoostedTrees和Logistic Regressor来设计和实现的
  * 首先训练特征并通过GradientBoostedTrees将其转换为稀疏矢量，然后在Logistic回归模型中训练和预测生成的稀疏特征
  *
  * @param uid 模型唯一id
  */
class GBTLRClassifier (override val uid: String)
  extends Predictor[Vector, GBTLRClassifier, GBTLRClassificationModel]
    with GBTLRClassifierParams with DefaultParamsWritable {

  import GBTLRClassifier._
  import GBTLRUtil._

  def this() = this(Identifiable.randomUID("gbtlr"))

  // Set GBTClassifier params

  /** @group setParam */
  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group setParam */
  def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group setParam */
  def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group setParam */
  def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group setParam */
  def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group setParam */
  def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

  /** @group setParam */
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /**
    * GBT模型不考虑不纯度设置
    * 单个树是使用不纯度方差构建的
    * @group setParam
    */
  def setImpurity(value: String): this.type = {
    logger.warn("不应使用GBTLRClassifier中的GBTC分类器")
    this
  }

  /** @group setParam */
  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  def setGBTMaxIter(value: Int): this.type = set(GBTMaxIter, value)

  /** @group setParam */
  def setStepSize(value: Double): this.type = set(stepSize, value)

  /** @group setParam */
  def setLossType(value: String): this.type = set(lossType, value)

  /** @group setParam */
  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)

  /** @group setParam */
  def setFamily(value: String): this.type = set(family, value)

  /** @group setParam */
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /** @group setParam */
  def setLRMaxIter(value: Int): this.type = set(LRMaxIter, value)

  /** @group setParam */
  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)

  /** @group setParam */
  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  /** @group setParam */
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** @group setParam */
  def setStandardization(value: Boolean): this.type = set(standardization, value)

  /** @group setParam */
  def setTol(value: Double): this.type = set(tol, value)

  /** @group setParam */
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /** @group setParam */
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)

  /** @group setParam */
  def setGbtGeneratedFeaturesCol(value: String): this.type = set(gbtGeneratedFeaturesCol, value)

  /**
    * 在二进制分类中设置阈值，范围为[0,1]
    *
    * 如果类标签1的估计概率大于阈值，则预测1，否则0
    * 高阈值鼓励模型更频繁地预测0;低阈值鼓励模型更频繁地预测1
    *
    * 注意: 用阈值p调用它相当于调用setThresholds(Array(1-p,p))
    *       当调用setThreshold()时，将清除thresholds的任何用户设置值
    * 如果在ParamMap中设置了“threshold”和“thresholds”，那么它们必须是等价的
    *
    * 默认是0.5.
    *
    * @group setParam
    */
  // TODO: Implement SPARK-11543?
  def setThreshold(value: Double): this.type = {
    if (isSet(thresholds)) clear(thresholds)
    set(threshold, value)
  }

  /**
    * 在多类(或二进制)分类中设置阈值，以调整预测每个类的概率。
    * 数组的长度必须等于类的数量，且值大于0，但最多有一个值可能为0
    * 预测p/t值最大值，其中p为初始概率，t是类的阈值
    *
    * 注意: 当调用setthreshold()时，threshold的任何用户设置值将被清除
    *       如果在参数映射中设置了阈值和阈值，那么它们必须是等价的
    *
    * @group setParam
    */
  def setThresholds(value: Array[Double]): this.type = {
    if (isSet(threshold)) clear(threshold)
    set(thresholds, value)
  }

  /**
    * 检查分类特征结构
    * @param featuresSchema 特征列结构
    *
    *                       如果某个要素没有元数据，则假定它是连续的。
    *
    *                       如果要素是标称值，则必须指定值的数量。
    * @return Map: 特征索引到类别数量
    *
    *         映射的键集将是分类特征索引集
    */
  private def getCategoricalFeatures(featuresSchema: StructField): Map[Int, Int] = {
    val metadata = AttributeGroup.fromStructField(featuresSchema)
    if (metadata.attributes.isEmpty) {
      HashMap.empty[Int, Int]
    } else {
      metadata.attributes.get.zipWithIndex.flatMap{ case (attr, idx) =>
        if (attr == null) {
          Iterator()
        } else {
          attr match {
            case _: NumericAttribute | UnresolvedAttribute => Iterator()
            case binAttr: BinaryAttribute => Iterator(idx -> 2)
            case nomAttr: NominalAttribute =>
              nomAttr.getNumValues match {
                case Some(numValues: Int) => Iterator(idx -> numValues)
                case None => throw new IllegalArgumentException(s"特征 $idx 是否标记为标称值(分类)，但没有指定值的数量.")
              }
          }
        }
      }.toMap
    }
  }

  /**
    * 创建要与旧API一起使用的策略实例
    * @param categoricalFeatures Map: feature index to number of categories.
    * @return Strategy instance
    */
  private def getOldStrategy(categoricalFeatures: Map[Int, Int]): OldStrategy = {
    val strategy = OldStrategy.defaultStrategy(OldAlgo.Classification)
    strategy.impurity = OldVariance
    strategy.checkpointInterval = getCheckpointInterval
    strategy.maxBins = getMaxBins
    strategy.maxDepth = getMaxDepth
    strategy.maxMemoryInMB = getMaxMemoryInMB
    strategy.minInfoGain = getMinInfoGain
    strategy.minInstancesPerNode = getMinInstancePerNode
    strategy.useNodeIdCache = getCacheNodeIds
    strategy.numClasses = 2
    strategy.categoricalFeaturesInfo = categoricalFeatures
    strategy.subsamplingRate = getSubsamplingRate
    strategy
  }

  /**
    * 获得旧的Gradient Boosting Loss类型
    * @return Loss type
    */
  private def getOldLossType: OldLoss = {
    getLossType match {
      case "logistic" => OldLogLoss
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTClassifier损失函数类型不匹配:" +
          s" $getLossType")
    }
  }

  /**
    * 训练GBTLRClassification模型
    * 该模型由GradientBoostedTreesModel和LogisticRegressionModel组成
    * @param dataset Input data.
    * @return GBTLRClassification model.
    */
  override def train(dataset: Dataset[_]): GBTLRClassificationModel = {
    val categoricalFeatures: Map[Int, Int] =
      getCategoricalFeatures(dataset.schema($(featuresCol)))

    // GBDT只支持二分类
    val oldDataset: RDD[OldLabeledPoint] =
      dataset.select(col($(labelCol)), col($(featuresCol))).rdd.map {
        case Row(label: Double, features: Vector) =>
          require(label == 0 || label == 1, s"GBTClassifier验证数据集无效标签 $label. " +
            s" 标签必须是0或1; 注意GBDT只支持二分类.")
          OldLabeledPoint(label, new OldDenseVector(features.toArray))
      }

    val numFeatures = oldDataset.first().features.size
    val strategy = getOldStrategy(categoricalFeatures)
    val boostingStrategy = new OldBoostingStrategy(strategy, getOldLossType,
      getGBTMaxIter, getStepSize)

    val instr = Instrumentation.create(this, oldDataset)
    instr.logParams(params: _*)
    instr.logNumFeatures(numFeatures)
    instr.logNumClasses(2)

    // 使用boostingStrategy训练梯度提升树模型
    val gbtModel = GradientBoostedTrees.train(oldDataset, boostingStrategy)

    // 用于创建由原始特征和GBDT生成特征组成的特征列
    val addFeatureUDF = udf { (features: Vector) =>
      val gbtFeatures = getGBTFeatures(gbtModel, features)
      Vectors.dense(features.toArray ++ gbtFeatures.toArray)
    }

    val datasetWithCombinedFeatures = dataset.withColumn($(gbtGeneratedFeaturesCol),
      addFeatureUDF(col($(featuresCol))))

    // 创建逻辑回归实例
    val logisticRegression = new LogisticRegression()
      .setRegParam($(regParam))
      .setElasticNetParam($(elasticNetParam))
      .setMaxIter($(LRMaxIter))
      .setTol($(tol))
      .setLabelCol($(labelCol))
      .setFeaturesCol($(featuresCol))
      .setFitIntercept($(fitIntercept))
      .setFamily($(family))
      .setStandardization($(standardization))
      .setPredictionCol($(predictionCol))
      .setProbabilityCol($(probabilityCol))
      .setRawPredictionCol($(rawPredictionCol))
      .setAggregationDepth($(aggregationDepth))
      .setFeaturesCol($(gbtGeneratedFeaturesCol))

    if (isSet(weightCol)) logisticRegression.setWeightCol($(weightCol))
    if (isSet(threshold)) logisticRegression.setThreshold($(threshold))
    if (isSet(thresholds)) logisticRegression.setThresholds($(thresholds))

    // 使用组合特征训练逻辑回归模型
    val lrModel = logisticRegression.fit(datasetWithCombinedFeatures)

    val model = copyValues(new GBTLRClassificationModel(uid, gbtModel, lrModel).setParent(this))
    val summary = new GBTLRClassifierTrainingSummary(datasetWithCombinedFeatures, lrModel.summary,
      gbtModel.trees, gbtModel.treeWeights)
    model.setSummary(Some(summary))
    instr.logSuccess(model)
    model
  }

  override def copy(extra: ParamMap): GBTLRClassifier = defaultCopy(extra)
}

object GBTLRClassifier extends DefaultParamsReadable[GBTLRClassifier] {

  val logger = Logger.getLogger(GBTLRClassifier.getClass)

  override def load(path: String): GBTLRClassifier = super.load(path)
}

class GBTLRClassificationModel (
                                 override val uid: String,
                                 val gbtModel: GradientBoostedTreesModel,
                                 val lrModel: LogisticRegressionModel)
  extends PredictionModel[Vector, GBTLRClassificationModel]
    with GBTLRClassifierParams with MLWritable {

  import GBTLRUtil._

  private var trainingSummary: Option[GBTLRClassifierTrainingSummary] = None

  private[gbtlr] def setSummary(
                                 summary: Option[GBTLRClassifierTrainingSummary]): this.type = {
    this.trainingSummary = summary
    this
  }

  /**
    * 如果存在模型则返回true
    */
  def hasSummary: Boolean = trainingSummary.nonEmpty

  def summary: GBTLRClassifierTrainingSummary = trainingSummary.getOrElse {
    throw new SparkException(
      s"没有有效的模型摘要 ${this.getClass.getSimpleName}"
    )
  }

  override def write: MLWriter =
    new GBTLRClassificationModel.GBTLRClassificationModelWriter(this)

  /**
    * 在给定特定特征点时，通过gbdt获取组合特征点
    * @param point Original one point.
    * @return A combined feature point.
    */
  def getComibinedFeatures(
                            point: OldLabeledPoint): OldLabeledPoint = {
    val numTrees = gbtModel.trees.length
    val treeLeafArray = new Array[Array[Int]](numTrees)
    for (i <- 0 until numTrees)
      treeLeafArray(i) = getLeafNodes(gbtModel.trees(i).topNode)

    var newFeature = new Array[Double](0)
    val label = point.label
    val features = point.features
    for (i <- 0 until numTrees) {
      val treePredict = predictModify(gbtModel.trees(i).topNode, features.toDense)
      val treeArray = new Array[Double]((gbtModel.trees(i).numNodes + 1) / 2)
      treeArray(treeLeafArray(i).indexOf(treePredict)) = 1
      newFeature = newFeature ++ treeArray
    }
    OldLabeledPoint(label.toInt, new OldDenseVector(features.toArray ++ newFeature))
  }

  // udf用于创建由原始特征和gbt模型生成的特征组成的特征列
  private val addFeatureUDF = udf { (features: Vector) =>
    val gbtFeatures = getGBTFeatures(gbtModel, features)
    Vectors.dense(features.toArray ++ gbtFeatures.toArray)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val datasetWithCombinedFeatures = dataset.withColumn($(gbtGeneratedFeaturesCol),
      addFeatureUDF(col($(featuresCol))))
    val predictions = lrModel.transform(datasetWithCombinedFeatures)
    predictions
  }

  // 仅在预测模型实现抽象方法，而不实现
  override def predict(features: Vector): Double = 0.0

  /**
    * 对测试数据集上的模型进行评估
    * @param dataset Test dataset to evalute model on.
    */
  def evaluate(dataset: Dataset[_]): GBTLRClassifierSummary = {
    val datasetWithCombinedFeatures = dataset.withColumn($(gbtGeneratedFeaturesCol),
      addFeatureUDF(col($(featuresCol))))
    val lrSummary = lrModel.evaluate(datasetWithCombinedFeatures)
    new GBTLRClassifierSummary(lrSummary)
  }

  override def copy(extra: ParamMap): GBTLRClassificationModel = {
    val copied = copyValues(new GBTLRClassificationModel(uid, gbtModel, lrModel), extra)
    copied.setSummary(trainingSummary).setParent(this.parent)
  }

  /**
    * 获取一组可以达到不同叶节点的规则
    * @param node Root node.
    * @param rule Current set of rules
    * @param rules Final set of rules of all leaf node.
    */
  private def getLeafRules(
                            node: OldNode,
                            rule: String,
                            rules: mutable.ArrayBuilder[String]) {
    val split = node.split
    if (node.isLeaf) {
      rules += rule
    } else {
      if (split.get.featureType == FeatureType.Continuous) {
        val leftRule = rule + s", feature#${split.get.feature} < ${split.get.threshold}"
        getLeafRules(node.leftNode.get, leftRule, rules)
        val rightRule = rule + s", feature#${split.get.feature} > ${split.get.threshold}"
        getLeafRules(node.rightNode.get, rightRule, rules)
      } else {
        val leftRule = rule + s", feature#${split.get.feature}'s value is in the Set" +
          split.get.categories.mkString("[", ",", "]")
        getLeafRules(node.leftNode.get, leftRule, rules)
        val rightRule = rule + s", feature#${split.get.feature}'s value is not in the Set" +
          split.get.categories.mkString("[", ",", "]")
        getLeafRules(node.rightNode.get, rightRule, rules)
      }
    }
  }

  /**
    * 通过lr训练权重获得额外特征的每个维度的描述
    * @return 一个tuple2数组，在每个tuple中，第一个elem表示额外特征的权重，第二个elem描述如何获得该特征
    */
  def getRules: Array[Tuple2[Double, String]] = {
    val numTrees = gbtModel.trees.length
    val rules = new Array[Array[String]](numTrees)
    var numExtraFeatures = 0
    for (i <- 0 until numTrees) {
      val rulesInEachTree = mutable.ArrayBuilder.make[String]
      getLeafRules(gbtModel.trees(i).topNode, "", rulesInEachTree)
      val rule = rulesInEachTree.result()
      numExtraFeatures += rule.length
      rules(i) = rule
    }
    val weightsInLR = lrModel.coefficients.toArray
    val extraWeights =
      weightsInLR.slice(weightsInLR.length - numExtraFeatures, weightsInLR.length)
    extraWeights.zip(rules.flatMap(x => x))
  }

}

object GBTLRClassificationModel extends MLReadable[GBTLRClassificationModel] {

  val logger = Logger.getLogger(GBTLRClassificationModel.getClass)


  override def read: MLReader[GBTLRClassificationModel] = new GBTLRClassificationModelReader

  override def load(path: String): GBTLRClassificationModel = super.load(path)

  private[GBTLRClassificationModel] class GBTLRClassificationModelWriter(
                                                                          instance: GBTLRClassificationModel) extends MLWriter {
    override def saveImpl(path: String) {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data
      val gbtDataPath = new Path(path, "gbtData").toString
      instance.gbtModel.save(sc, gbtDataPath)
      val lrDataPath = new Path(path, "lrData").toString
      instance.lrModel.save(lrDataPath)
    }
  }

  private class GBTLRClassificationModelReader
    extends MLReader[GBTLRClassificationModel] {

    private val className = classOf[GBTLRClassificationModel].getName

    override def load(path: String): GBTLRClassificationModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val gbtDataPath = new Path(path, "gbtData").toString
      val lrDataPath = new Path(path, "lrData").toString
      val gbtModel = GradientBoostedTreesModel.load(sc, gbtDataPath)
      val lrModel = LogisticRegressionModel.load(lrDataPath)
      val model = new GBTLRClassificationModel(metadata.uid, gbtModel, lrModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

class GBTLRClassifierTrainingSummary (
                                       @transient val newDataset: DataFrame,
                                       val logRegSummary: LogisticRegressionTrainingSummary,
                                       val gbtTrees: Array[DecisionTreeModel],
                                       val treeWeights: Array[Double]) extends Serializable {
}

class GBTLRClassifierSummary (
                               val binaryLogisticRegressionSummary: LogisticRegressionSummary)
  extends Serializable {
}


object GBTLRUtil {
  /**
    * 根据树的根节点获取叶节点数组
    * 数组中节点的顺序是从左到右
    *
    * @param node 树的根结点
    * @return 数组存储叶节点id
    */
  def getLeafNodes(node: OldNode): Array[Int] = {
    var treeLeafNodes = new Array[Int](0)
    if (node.isLeaf) {
      treeLeafNodes = treeLeafNodes :+ (node.id)
    } else {
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.leftNode.get)
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.rightNode.get)
    }
    treeLeafNodes
  }

  /**
    * 获取特征将位于的叶节点id
    *
    * @param node 树的根结点
    * @param features 稠密矩阵特征
    * @return 叶节点id
    */
  def predictModify(node: OldNode, features: OldDenseVector): Int = {
    val split = node.split
    if (node.isLeaf) {
      node.id
    } else {
      if (split.get.featureType == FeatureType.Continuous) {
        if (features(split.get.feature) <= split.get.threshold) {
          predictModify(node.leftNode.get, features)
        } else {
          predictModify(node.rightNode.get, features)
        }
      } else {
        if (split.get.categories.contains(features(split.get.feature))) {
          predictModify(node.leftNode.get, features)
        } else {
          predictModify(node.rightNode.get, features)
        }
      }
    }
  }

  /**
    *从GBT模型中获取GBT生成的特征
    *
    * @param gbtModel
    * @param features
    * @return
    */
  def getGBTFeatures(gbtModel: GradientBoostedTreesModel, features: Vector): Vector = {
    val GBTMaxIter = gbtModel.trees.length
    val oldFeatures = new OldDenseVector(features.toArray)
    val treeLeafArray = new Array[Array[Int]](GBTMaxIter)
    for (i <- 0 until GBTMaxIter)
      treeLeafArray(i) = getLeafNodes(gbtModel.trees(i).topNode)
    var newFeature = new Array[Double](0)
    for (i <- 0 until GBTMaxIter) {
      val treePredict = predictModify(gbtModel.trees(i).topNode, oldFeatures.toDense)
      val treeArray = new Array[Double]((gbtModel.trees(i).numNodes + 1) / 2)
      treeArray(treeLeafArray(i).indexOf(treePredict)) = 1
      newFeature = newFeature ++ treeArray
    }
    Vectors.dense(newFeature)
  }
}
