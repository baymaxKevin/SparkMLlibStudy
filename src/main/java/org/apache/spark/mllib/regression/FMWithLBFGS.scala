package org.apache.spark.mllib.regression

import org.apache.spark.mllib.linalg.{DenseMatrix, Vector, Vectors}
import org.apache.spark.mllib.optimization.LBFGS
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

/**
  * @Author: JZ.lee
  * @Description: TODO
  * @Date: 18-12-4 上午11:02
  * @Modified By:
  */
object FMWithLBFGS {
  /**
    * 在给出(标签,特征)对的RDD的情况下训练Factoriaton机器回归模型
    * 我们使用指定的步长运行固定数量的梯度下降迭代
    * 每次迭代使用数据的“miniBatchFraction”部分来计算随机梯度
    * 使用提供的初始权重初始化梯度下降中使用的权重
    *
    * @param input (标签,特征数组)对的RDD,每一对描述数据矩阵a的一行以及对应的右侧标签y
    * @param task 回归设置为0, 分类设置为1
    * @param numIterations 要运行的梯度下降迭代次数
    * @param dim A (Boolean,Boolean,Int) 3元组分别表示是否应该使用全局偏差项，是否应该使用单向交互，以及用于成对交互的因子数量
    * @param regParam A (Double,Double,Double) 3元组分别表示截获、单向交互和双向交互的正则化参数
    * @param initStd 用于分解矩阵初始化的标准偏差
    */
  def train(input: RDD[LabeledPoint],
            task: Int,
            numIterations: Int,
            numCorrections: Int,
            dim: (Boolean, Boolean, Int),
            regParam: (Double, Double, Double),
            initStd: Double): FMModel = {
    new FMWithLBFGS(task, numIterations, numCorrections, dim, regParam)
      .setInitStd(initStd)
      .run(input)
  }

  //  def train(input: RDD[LabeledPoint],
  //            task: Int,
  //            numIterations: Int): FMModel = {
  //    new FMWithSGD(task, 1.0, numIterations, (true, true, 8), (0, 0.01, 0.01), 1.0)
  //      .setInitStd(0.01)
  //      .run(input)
  //  }
}


class FMWithLBFGS(private var task: Int,
                  private var numIterations: Int,
                  private var numCorrections: Int,
                  private var dim: (Boolean, Boolean, Int),
                  private var regParam: (Double, Double, Double)) extends Serializable {

  private var k0: Boolean = dim._1
  private var k1: Boolean = dim._2
  private var k2: Int = dim._3

  private var r0: Double = regParam._1
  private var r1: Double = regParam._2
  private var r2: Double = regParam._3

  private var initMean: Double = 0
  private var initStd: Double = 0.01

  private var numFeatures: Int = -1
  private var minLabel: Double = Double.MaxValue
  private var maxLabel: Double = Double.MinValue

  /**
    * A  (Boolean,Boolean,Int) 3元组分别表示是否应该使用全局偏差项，是否应该使用单向交互，以及用于成对交互的因子数量
    */
  def setDim(dim: (Boolean, Boolean, Int)): this.type = {
    require(dim._3 > 0)
    this.k0 = dim._1
    this.k1 = dim._2
    this.k2 = dim._3
    this
  }

  /**
    *
    * @param addIntercept 确定是否应该使用全局偏差项w0
    * @param add1Way 确定是否单向交互(每个变量的偏差项)
    * @param numFactors 用于成对交互的因子的数量
    */
  def setDim(addIntercept: Boolean = true, add1Way: Boolean = true, numFactors: Int = 8): this.type = {
    setDim((addIntercept, add1Way, numFactors))
  }


  /**
    * @param regParams A (Double,Double,Double) 3元组分别表示截获、单向交互和双向交互的正则化参数
    */
  def setRegParam(regParams: (Double, Double, Double)): this.type = {
    require(regParams._1 >= 0 && regParams._2 >= 0 && regParams._3 >= 0)
    this.r0 = regParams._1
    this.r1 = regParams._2
    this.r2 = regParams._3
    this
  }

  /**
    * @param regIntercept 截获正则化
    * @param reg1Way 单向交互正则化
    * @param reg2Way 双向交互正则化
    */
  def setRegParam(regIntercept: Double = 0, reg1Way: Double = 0, reg2Way: Double = 0): this.type = {
    setRegParam((regIntercept, reg1Way, reg2Way))
  }


  /**
    * @param initStd 用于分解矩阵初始化的标准偏差
    */
  def setInitStd(initStd: Double): this.type = {
    require(initStd > 0)
    this.initStd = initStd
    this
  }


  /**
    * 设置SGD的迭代次数
    */
  def setNumIterations(numIterations: Int): this.type = {
    require(numIterations > 0)
    this.numIterations = numIterations
    this
  }


  /**
    * 将FMModel编码为一个密集向量，其第一个numFeatures * numFactors元素表示分解矩阵v
    * 如果k1为真，则顺序numFeatures元素表示单向交互权重w，如果k0为真，则最后一个元素表示截距w0
    * 分解矩阵v由Gaussinan(0, initStd)初始化
    * v : numFeatures * numFactors + w : [numFeatures] + w0 : [1]
    */
  private def generateInitWeights(): Vector = {
    (k0, k1) match {
      case (true, true) =>
        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean) ++
          Array.fill(numFeatures + 1)(0.0))

      case (true, false) =>
        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean) ++
          Array(0.0))

      case (false, true) =>
        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean) ++
          Array.fill(numFeatures)(0.0))

      case (false, false) =>
        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean))
    }
  }


  /**
    * 从编码向量创建FMModel
    */
  private def createModel(weights: Vector): FMModel = {

    val values = weights.toArray

    val v = new DenseMatrix(k2, numFeatures, values.slice(0, numFeatures * k2))

    val w = if (k1) Some(Vectors.dense(values.slice(numFeatures * k2, numFeatures * k2 + numFeatures))) else None

    val w0 = if (k0) values.last else 0.0

    new FMModel(task, v, w, w0, minLabel, maxLabel)
  }


  /**
    * 在LabeledPoint条目的输入RDD上运行带有配置参数的算法
    */
  def run(input: RDD[LabeledPoint]): FMModel = {

    if (input.getStorageLevel == StorageLevel.NONE) {
      println("输入数据不会直接缓存，如果其父RDD也未缓存，则可能会影响性能.")
    }

    this.numFeatures = input.first().features.size
    require(numFeatures > 0)

    if (task == 0) {
      val (minT, maxT) = input.map(_.label).aggregate[(Double, Double)]((Double.MaxValue, Double.MinValue))({
        case ((min, max), v) =>
          (Math.min(min, v), Math.max(max, v))
      }, {
        case ((min1, max1), (min2, max2)) =>
          (Math.min(min1, min2), Math.max(max1, max2))
      })

      this.minLabel = minT
      this.maxLabel = maxT
    }

    val gradient = new FMGradient(task, k0, k1, k2, numFeatures, minLabel, maxLabel)

    val updater = new FMUpdater(k0, k1, k2, r0, r1, r2, numFeatures)

    val optimizer = new LBFGS(gradient, updater)
      .setNumIterations(numIterations)

    val data = task match {
      case 0 =>
        input.map(l => (l.label, l.features)).persist()
      case 1 =>
        input.map(l => (if (l.label > 0) 1.0 else -1.0, l.features)).persist()
    }

    val initWeights = generateInitWeights()

    val weights = optimizer.optimize(data, initWeights)

    data.unpersist()

    createModel(weights)
  }
}
