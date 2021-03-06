package com.sparkMLlibStudy.model

import java.util

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.ml.stat.{ChiSquareTest, Correlation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.functions.col


/**
  *
  */
object BasicStatisticsModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    /**
      * 计算皮尔逊线性相关系数和皮尔斯曼系数、卡方检验
      */
    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
//    df.show()
//    df.printSchema()
    /**
      * pearson = E(X)E(Y)/(E(x2)-E2(x))1/2*(E(Y2)-E2(Y))1/2
      * 变量服从正太分布
      */
    val Row(coff1: Matrix) = Correlation.corr(df,
      "features").head
//    println(s"Pearson correlation matrix:\n $coff1")

    val Row(coff2: Matrix) = Correlation.corr(df,
      "features", "spearman").head
//    println(s"Spearman correlation matrix:\n $coff2")

    /**
      * 卡方检验
      */
    val data1 = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )

    val df1 = spark.createDataFrame(data1).toDF("label","features")
    val chi = ChiSquareTest.test(df1, "features",
      "label").head
//    println(s"pValues = ${chi.getAs[Vector](0)}")
//    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[",",","]")}")
//    println(s"statistics ${chi.getAs[Vector](2)}")

    /**
      * tf-idf(term frequency-inverse document frequency)
      * 一种广泛用于文本挖掘的特征向量方法，用户反映术语对语料库中文档重要性
      * tf(Term Frequency):表示一个term与某个document的相关性
      * idf(Inverse Document Frequency):表示一个term表示document的主题的权重大小
      * tf(t,d)词频
      * idf(t,D)=log((|D|+1)/(DF(t,D)+1))，其中|D|表示文件集总数，DF词出现(t,D)文档数量
      * tfidf(t,d,D)=tf(t,d)*idf(t,D)
      * 示例：
      * 一篇文件总词语是100个，词语“胡歌”出现了3次，那么“胡歌”一词在该文件中的词频TF(t,d)=3/100；
      * 如果“胡歌”一词在1000份文件中出现过，而文件总数是10000，其文件频率DF(t,D)=log((10000+1)/(1000+1))
      * 那么“胡歌”一词在该文件集的tf-idf分数为TF(t,d)*DF(t,D)
      */
    val sentence = spark.createDataFrame(
      Seq(
        (0.0, "Hi I heard about Spark"),
        (0.0, "I wish Java could use case classes"),
        (1.0, "Logistic regression models are neat")
      )
    ).toDF("label","sentence")

    val tk = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val words = tk.transform(sentence)
//    words.show()

    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(20)

    val featurized = hashingTF.transform(words)
//    featurized.show()

    val idf = new IDF().setInputCol("rawFeatures")
      .setOutputCol("features")
    val idfModel = idf.fit(featurized)

    val rescaled = idfModel.transform(featurized)
//    rescaled.show()

    /**
      * word2vec：采用代表文档的单词序列训练word2VecModel
      * word2vec该模型将每个单词映射到唯一固定长度向量，此向量用于预测，文档相似度计算等
      */
    val documentDF = spark.createDataFrame(
      Seq(
        "Hi I heard about Spark".split(" "),
        "I wish Java could use case classes".split(" "),
        "Logistic regression models are neat".split(" ")
      ).map(Tuple1.apply)
    ).toDF("text")

//    词映射到向量
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
//    result.collect().foreach{
//      case Row(text: Seq[_],features: Vector)=>
//        println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
//    }

    /**
      * CountVectorizer and CountVectorizerModel
      * 旨在通过计数将一个文档转换为向量
      * 当不存在先验字典，CountVectorizer可以作为Estimator提取词汇，并生成CountVectorizerModel
      * 该模型产生关于文档词汇的稀疏特征向量，可以传递给其他像LDA算法
      */

    val df2 = spark.createDataFrame(
      Seq(
        (0, Array("a","b","c")),
        (1,Array("a","b","c","a"))
      )
    ).toDF("id","words")

//    拟合语料库CountVectorizerModel
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df2)

//    使用先验词表定义CountVectorizerModel
    val cvm = new CountVectorizerModel(Array("a","b","c"))
      .setInputCol("words")
      .setOutputCol("features")

//    cvModel.transform(df2).show(false)

    /**
      * FeatureHasher
      * 特征散列将一组分类或数字特征投影到指定纬度的特征向量中(通常小于原始特征空间的特征向量)
      * 数字特征
      * 字符串特征：onehot编码
      */
    val df3 = spark.createDataFrame(
      Seq(
        (2.2, true, "1", "foo"),
        (3.3, false, "2", "bar"),
        (4.4, false, "3", "baz"),
        (5.5, false, "4", "foo")
      )
    ).toDF("real","bool","stringNum","string")

    val hasher = new FeatureHasher()
      .setInputCols("real","bool","stringNum","string")
      .setOutputCol("features")

    val featurizedHash = hasher.transform(df3)
//    featurizedHash.show(false)

    /**
      * 分词器：文本拆分为单个术语(单词)的过程
      * RegexTokenizer允许基于正则表达式匹配更高级标记化
      */
    val tkz = new Tokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W")

    val countTk = functions.udf{ (words: Seq[String]) =>words.length}
    val tokenizer = tkz.transform(sentence)
    tokenizer.select("sentence","words")
      .withColumn("tokens", countTk(tokenizer("words")))
//      .show(false)

    val regexTokenized = regexTokenizer.transform(sentence)
    regexTokenized.select("sentence", "words")
      .withColumn("tokens",countTk(regexTokenized("words")))
//      .show(false)

    /**
      * stopwords
      * 停止词是应该从输入中排除的词，通常是因为词经常出现并且没有那么多含义
      */
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val data2 = spark.createDataFrame(
      Seq(
        (0, Seq("I","saw","the","red","balloon")),
        (1, Seq("Mary", "had", "a", "little", "lamb"))
      )
    ).toDF("id", "raw")

//    remover.transform(data2).show(false)

    /**
      * n-gram代表由n个字组成的句子
      * 利用上下文中相邻词间的搭配信息，
      * 在需要把连续无空格的拼音、笔划，或代表字母或笔划的数字，
      * 转换成汉字串(即句子)时，可以计算出具有最大概率的句子，
      * 从而实现到汉字的自动转换，无需用户手动选择，
      * 避开了许多汉字对应一个相同的拼音(或笔划串，或数字串)的重码问题。
      * 该模型基于这样一种假设，
      * 第N个词的出现只与前面N-1个词相关，
      * 而与其它任何词都不相关，整句的概率就是各个词出现概率的乘积。
      */
    val ngram = new NGram().setN(2)
      .setInputCol("raw")
      .setOutputCol("ngrams")

    val ngramDF = ngram.transform(data2)
//    ngramDF.select("ngrams").show(false)

    /**
      * 二值化：将数值特征阈值化为二进制(0/1)特征过程
      * Binarizer采用公共参数inputCol和outputCol以及二值化的阈值
      * 大于阈值的特征被二进制为1，等于或小于阈值的特征被二值化为0
      */
    val data3 = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data3).toDF("id", "feature")
    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)
//    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
//    binarizedDataFrame.show()

    /**
      * PCA
      * 通过正交变换将线性相关变量转换为线性不相关变量
      */
    val data4 = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df4= spark.createDataFrame(data4.map
    (Tuple1.apply)).toDF("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df4)

    val res = pca.transform(df4).select("pcaFeatures")
//    res.show(false)

    /**
      * PolynomialExpansion：多项式扩展
      */
    val polyExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)

    val polyDF = polyExpansion.transform(df)
//    polyDF.show(false)

    /**
      * Discrete Cosine Transform(DCT):离散余弦变换
      * 离散余弦变换将时域中长度为N实值序列变换为频域中另一长度为N实值序列
      */
    val dct = new DCT()
      .setInputCol("features")
      .setOutputCol("featuresDCT")
      .setInverse(false)

    val dctDF = dct.transform(df4)
//    dctDF.select("featuresDCT").show(false)

    /**
      * StringIndexer:
      * 将一列字符串标签编码成一列下标标签，下标范围是[0,标签数量)
      * 顺序是标签的出现频率
      * IndexToString:
      * 和StringIndexer是对称的，将一列下标标签映射回一列包含原始字符串的标签
      * 常用于StringIndexer生产下标，通过下标训练模型，通过IndexToString
      * 从预测出下标列重新获得原始标签
      */
    val data5 = spark.createDataFrame(
      Seq((0, "a"),(1, "b"),(2, "c"),(3, "a"),(4, "a"),
        (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(data5).transform(data5)
//    println(s"Transformed string column '${indexer
//      .getInputCol}'" + s"to indexed column '${indexer
//      .getOutputCol}'")
//    indexed.show(false)

    val inputColSchema = indexed.schema(indexer.getOutputCol)
//    println(s"StringIndexer will store labels in output " +
//      s"column meatadata:${Attribute.fromStructField(inputColSchema).toString()}\n")

    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)
//    println(s"Transformed indexed column '${converter.getInputCol}' back to original string " +
//      s"column '${converter.getOutputCol}' using labels in metadata")
//    converted.select("id", "categoryIndex", "originalCategory").show()

    /**
      * OneHot编码
      * 将表示为标签索引的分类特征映射到二进制向量
      * 此编码允许期望连续特征的算法使用分类特征(Logistic回归)的算法使用分类特征
      */
    val data6 = spark.createDataFrame(
      Seq(
        (0.0, 1.0),
        (1.0, 0.0),
        (2.0, 1.0),
        (0.0, 2.0),
        (0.0, 1.0),
        (2.0, 0.0)
      )
    ).toDF("categoryIndex1","categoryIndex2")

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
      .setOutputCols(Array("categoryVec1","categoryVec2"))
    val modelOH = encoder.fit(data6)

    val encoded = modelOH.transform(data6)
//    encoded.show(false)

    /**
      * VectorIndexer
      * 对数据集特征向量中的类别(离散值)特征(index categorical features
      * categorial features)进行编码；
      * 提高决策树或随机森林等ML方法的分类效果
      */
    val data7 = spark.read.format("libsvm").load("/opt/modules/spark-2.3.1/data/mllib/sample_libsvm_data.txt")

    val indexer1 = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10)

    val indexerModel = indexer1.fit(data7)
    val categoricalFeatures:Set[Int] = indexerModel.categoryMaps.keys.toSet
//    println(s"Chose ${categoricalFeatures.size} " +
//      s"categorical features: ${categoricalFeatures.mkString(", ")}")

//    使用转换为索引的分类值创建索引新列
    val indexedData = indexerModel.transform(data7)
//    indexedData.show(false)

    /**
      * interaction
      * transformer,接受向量或双值列，并生成一个向量列，其中包含每个输入列的一个值
      */
    val data8 = spark.createDataFrame(Seq(
      (1, 1, 2, 3, 8, 4, 5),
      (2, 4, 3, 8, 7, 9, 8),
      (3, 6, 1, 9, 2, 3, 6),
      (4, 10, 8, 6, 9, 4, 5),
      (5, 9, 2, 7, 10, 7, 3),
      (6, 1, 1, 4, 2, 8, 4)
    )).toDF("id1", "id2", "id3", "id4", "id5", "id6", "id7")
    val assembler1 = new VectorAssembler()
      .setInputCols(Array("id2", "id3", "id4"))
      .setOutputCol("vec1")

    val assembled1 = assembler1.transform(data8)

    val assembler2 = new VectorAssembler()
      .setInputCols(Array("id5", "id6", "id7"))
      .setOutputCol("vec2")

    val assembled2 = assembler2.transform(assembled1)
      .select("id1", "vec1", "vec2")

    val interaction = new Interaction()
      .setInputCols(Array("id1", "vec1", "vec2"))
      .setOutputCol("interactedCol")

    val interacted = interaction.transform(assembled2)

//    interacted.show(false)

    /**
      * Normalizer正则化
      * Normalizer是一个转换器，它可以将多行向量输入转化为统一的形式
      * 参数为p（默认值：2）来指定正则化中使用的p-norm
      * 正则化操作可以使输入数据标准化并提高后期学习算法的效果
      */
    val data9 = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

//    使用L1正则标准化每列向量
    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0)

    val l1NormData = normalizer.transform(data9)
//    println("Normalized using L^1 norm")
//    l1NormData.show()

//    无限大正则
    val lInfNormData = normalizer.transform(data9, normalizer.p -> Double.PositiveInfinity)
//    println("Normalized using L^inf norm")
//    lInfNormData.show()

    /**
      * MinMaxScaler
      * 标准化
      */
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

//    创建MinMaxScalerModel
    val scalerModel = scaler.fit(data9)

//    [min,max]标准化
    val scaled = scalerModel.transform(data9)
//    println(s"Features scaled to range: [${scaler.getMin}, ${scaler.getMax}]")
//    scaled.select("features", "scaledFeatures").show(false)

    /**
      * MaxAbsScaler
      * 通过每列最大绝对值归一化[-1,1]
      */
    val scalerMA = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // 创建MaxAbsScalerModel
    val scml = scalerMA.fit(data9)

    // 归一化至[-1, 1]
    val scaledData = scml.transform(data9)
//    scaledData.select("features", "scaledFeatures").show(false)

    /**
      * Bucketizer
      * 分箱(分段处理):连续值转换为离散类别
      */
    val splits = Array(Double.NegativeInfinity, -0.5,
      0.0, 0.5, Double.PositiveInfinity)
    val data10 = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val df5 = spark.createDataFrame(data10.map(Tuple1.apply))
      .toDF("features")

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    val bucketed = bucketizer.transform(df5)
//    println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
//    bucketed.show()

    /**
      * ElementwiseProduct
      * 对输入向量的每个元素乘以一个权重向量的每个元素，对输入向量每个元素逐个进行放缩
      */
    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("features")
      .setOutputCol("transformedVector")

//    批量转换矢量以创建新列
//    transformer.transform(data9).show()

    val df6 = spark.createDataFrame(
      Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")

//    sqlTrans.transform(df6).show()

    /**
      * VectorAssembler
      * 一个transformer，将多列数据转化为单列的向量列
      * 原始数据集里，经常会包含一些非指标数据，如 ID，Description 等
      * 为方便后续模型进行特征输入，需要部分列的数据转换为特征向量，并统一命名
      */
    val df7 = spark.createDataFrame(
      Seq(
        (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
        (0, 18, 1.0, Vectors.dense(0.0, 10.0), 0.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour","mobile","userFeatures"))
      .setOutputCol("features")

    val output = assembler.transform(df7)
//    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
//    output.select("features", "clicked").show(false)

    /**
      * VectorSizeHint
      * VectorSizeHint允许用户显式指定列的向量大小
      * 以便VectorAssembler或可能需要知道向量大小的其他变换器可以将该列用作输入
      */
    val sizeHint = new VectorSizeHint()
      .setInputCol("userFeatures")
      .setHandleInvalid("skip")
      .setSize(3)

    val datasetWithSize = sizeHint.transform(df7)
//    println("Rows where 'userFeatures' is not the right size are filtered out")
//    datasetWithSize.show(false)

    val outputHint = assembler.transform(datasetWithSize)
//    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
//    output.select("features", "clicked").show(false)

    /**
      * QuantileDiscretizer
      * 采用具有连续特征的列，并输出具有分箱分类特征的列
      */
    val df8 = spark.createDataFrame(
      Array((0, 18.0), (1, 19.0), (2, 8.0), (3, 5.0), (4, 2.2))
    ).toDF("id", "hour")
    val discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3)

    val ret = discretizer.fit(df8).transform(df8)
//    ret.show(false)

    /**
      * Imputer使用缺失值所在的列的平均值或中值来完成数据集中的缺失值
      * 输入列应为DoubleType或FloatType
      */
    val df9 = spark.createDataFrame(
      Seq(
        (1.0, Double.NaN),
        (2.0, Double.NaN),
        (Double.NaN, 3.0),
        (4.0, 4.0),
        (5.0, 5.0)
      )
    ).toDF("a", "b")

    val imputer = new Imputer()
      .setInputCols(Array("a","b"))
      .setOutputCols(Array("out_a","out_b"))

    val modelIm = imputer.fit(df9)
//    modelIm.transform(df9).show(false)

    /**
      * VectorSlicer
      * 一个转换器输入特征向量，输出原始特征向量子集
      * VectorSlicer接收带有特定索引的向量列，通过对这些索引的值进行筛选得到新的向量集
      */
    val data11 = util.Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0))
    )
    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val df10 = spark.createDataFrame(data11, StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
    slicer.setIndices(Array(1)).setNames(Array("f3"))

    val outputSL = slicer.transform(df10)
//    outputSL.show(false)

    /**
      *  RFormula
      *  RFormula通过R模型公式来选择列。支持R操作中的部分操作，包括‘~’, ‘.’, ‘:’, ‘+’以及‘-‘
      *  产生一个向量特征列以及一个double或者字符串标签列
      */
    val df11 = spark.createDataFrame(Seq(
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")

    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val outputFR = formula.fit(df11).transform(df11)
//    outputFR.select("features", "label").show(false)

    /**
      * ChiSqSelector：卡方特征选择
      * 适用于带有类别特征的标签数据
      * ChiSqSelector根据独立卡方检验，然后选取类别标签主要依赖的特征
      * 它类似于选取最有预测能力的特征
      */
    val df12 = spark.createDataFrame(
      Seq(
        (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
        (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
        (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
      )
    ).toDF("id", "features", "clicked")

    val selector = new ChiSqSelector()
      .setNumTopFeatures(1)
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures")

    val relt = selector.fit(df12).transform(df12)
//    relt.show(false)

    /**
      * Locality Sensitive Hashing (LSH)
      * 一类重要的散列技术，常用于聚类，近似最近邻搜索和大数据集的异常检测
      * 使用一系列函数（“LSH系列”）将数据点哈希到桶中，使得彼此接近的数据点在相同的桶中具有高概率，而数据点是远离彼此很可能在不同的桶中
      * 在度量空间（M，d）中，其中M是集合，d是M上的距离函数，LSH族是满足以下属性的函数族h：
      * ∀p,q∈M
      * d(p,q)≤r1⇒Pr(h(p)=h(q))≥p1
      * d(p,q)≥r2⇒Pr(h(p)=h(q))≤p2
      * 则(r1, r2, p1, p2)-sensitive
      * 1.Bucketed Random Projection for Euclidean Distance
      * d(x,y) = sqrt(X,Y) = sqrt(sum((xi-yi)^2))
      * h(x) = |xv/r|
      * r是用户定义的桶长度
      * 桶长度可以用来控制散列桶的平均大小（从而控制桶的数量）
      * 2.MinHash for Jaccard Distance
      * MinHash是用于计算Jaccard距离的LSH族
      * d(A,B) = 1 -|A ∩ B| / |A ∪ B|
      * MinHash 对集合中的每个元素应用随机哈希函数g，并取所有哈希值的最小值：
      * h(A) = min(g(a))  ,a∈A
      * https://www.cnblogs.com/maybe2030/p/4953039.html
      * */
    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 1.0)),
      (1, Vectors.dense(1.0, -1.0)),
      (2, Vectors.dense(-1.0, -1.0)),
      (3, Vectors.dense(-1.0, 1.0))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (4, Vectors.dense(1.0, 0.0)),
      (5, Vectors.dense(-1.0, 0.0)),
      (6, Vectors.dense(0.0, 1.0)),
      (7, Vectors.dense(0.0, -1.0))
    )).toDF("id", "features")

    val key = Vectors.dense(1.0, 0.0)

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("features")
      .setOutputCol("hashes")

    val modelBR = brp.fit(dfA)
//    特征转换
//    println("The hashed dataset where hashed values are stored in the column 'hashes':")
//    modelBR.transform(dfA).show(false)

//    计算输入行的局部敏感哈希值，然后执行近似值相似性加入
//    我们可以通过传入已经转换过的数据集来避免计算哈希值
//    1.欧几里得距离
//    println("Approximately joining dfA and dfB on Euclidean distance smaller than 1.5:")
//    modelBR.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
//      .select(col("datasetA.id").alias("idA"), col("datasetB.id").alias("idB"),
//        col("EuclideanDistance")).show(false)

//    println("Approximately searching dfA for 2 nearest neighbors of the key:")
//    modelBR.approxNearestNeighbors(dfA, key, 2).show(false)

    val mh = new MinHashLSH()
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")

    val modelMH = mh.fit(dfA)

//    println("The hashed dataset where hashed values are stored in the column 'hashes':")
//    modelMH.transform(dfA).show(false)

//    println("Approximately joining dfA and dfB on Jaccard distance smaller than 0.6:")
//    modelMH.approxSimilarityJoin(dfA, dfB, 0.6, "JaccardDistance")
//      .select(col("datasetA.id").alias("idA"),
//        col("datasetB.id").alias("idB"),
//        col("JaccardDistance")).show(false)

//    println("Approximately searching dfA for 2 nearest neighbors of the key:")
//    modelMH.approxNearestNeighbors(dfA, key, 2).show(false)


  }
}

