package com.sparkStudy.chapter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object SparkSql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.ERROR)

    /**
      * Spark SQL所有功能入口点是SparkSession，创建SparkSession
      */
    val spark = SparkSession
      .builder()
      .appName("SparkSql")
      .master("local[2]")
      .config("spark.some.config.option", "some-value")
      .enableHiveSupport()
      .getOrCreate()

    /**
      * 读取json创建dataframe
      */
    val df1 = spark.read.json("/opt/data/sample_example.json")

    /**
      * 打印dataframe表和结构
      */
//    df1.show()
//    df1.printSchema()
    /**
      * 使用functions.explode展开数组
      */
    val df2 = df1.select(df1("name"),functions.explode
    (df1("myScore"))).toDF("name","myScore")
//    df2.show()
//    df2.printSchema()
//    df2.select("myScore.score1").show()
//    df2.select(df2("name"),df2("myScore.score2") + 1).show()
//    df2.filter(df2("myScore.score2") > 60).show()
//    df2.groupBy("name").count().show()

    /**
      * 创建临时试图(session级别)和全局临时视图
      */
    df2.createOrReplaceTempView("score")
    df2.createGlobalTempView("global_score")
    val df3 = spark.sql("select * from score")
//    df3.show()
    /**
      * dataframe通过类转换为dataset
      */
    import spark.implicits._
    val df4 = spark.read.json("/opt/data/sample.json").as[PageView]
//    df4.show()
    /**
      * 将元素类型为case class的RDD自动转化为DataFrame,case class定义表模式
      * row=>Map[K,V],注意org.apache.spark.sql.Encoders.kryo隐式转换
      */
    val df5 = spark
      .sparkContext
      .textFile("/opt/data/keyword_catalog_day.csv")
      .map(_.split(","))
      .map(attributes=>Keyword(attributes(0),attributes
      (1).toInt,attributes(3),attributes(4)
        .toDouble,attributes(5).toDouble)).toDF()

    df5.createOrReplaceTempView("keyword")
    val df6 = spark.sql("select keyword,app_id," +
      "catalog_name,keyword_catalog_pv," +
      "keyword_catalog_pv_rate from keyword where app_id " +
      "= 1")
//    df6.map(row => "keyword:" + row(0)).show()
//    df6.map(row => "keyword:" + row.getAs[String]
//      ("keyword")).show()
    implicit val mapEncode = org.apache.spark.sql.Encoders.kryo[Map[String,Any]]
//    df6.map(row => row.getValuesMap[Any](List("keyword",
//      "keyword_catalog_pv_rate"))).collect().foreach(println(_))

    /**
      * 不能预先定义case class,采用编码指定模式:
      * 1.原始RDD=>Row RDD
      * 2.根据row结构创建对应的StructType模式
      * 3.通过SparkSession提供的createDataFrame来把第2
      * 步创建模式应用到第一部Row RDD中
      */
    val schemaString = "keyword,app_id,catalog_name," +
      "keyword_catalog_pv,keyword_catalog_pv_rate"
    val fields = schemaString.split(",")
      .map(fieldName=>StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rd1 = spark.sparkContext
      .textFile("/opt/data/keyword_catalog_day.csv")
      .map(_.split(","))
      .map(row=>Row(row(0),row(1),row(2),row(3),row(4)))
    val df7 = spark.createDataFrame(rd1,schema)
    df7.createOrReplaceTempView("keyword_catalog_day")
    val df8 = spark.sql("select * from keyword_catalog_day")
//    df8.show()

    /**
      * 通用加载(json,parquet,jdbc,orc,libsvm,csv,text)
      *
      */
    val df9 = spark.read.load("/opt/data/users.parquet")
//    df9.select("name","favorite_color").write.save("nameAndFavoriteColors.parquet")
    /**
      * save mode:
      * SaveMode.ErrorIfExists=>将dataframe保存到data
      * source，如果数据源已经存在，则会抛异常
      * SaveMode.Append=>如果数据源已经存在，则会追加现有数据
      * SaveMode.OverWrite=>覆盖数据
      * SaveMode.Ignore=>则保存数据，并不更改现有内容，相当于create table
      * if not exists *
      */
//    df9.select("name","favorite_color").write.mode(SaveMode.Append).save("nameAndFavoriteColors.parquet")

    /**
      * 基于文件数据源，可以对output进行partition/bucket/sort
      * bucket和sort仅适用persistent table
      * 通过分区提高spark运行性能：https://www.iteblog.com/archives/1695.html
      */
    df5.write
      .mode(SaveMode.Overwrite)
      .partitionBy("app_id")
      .bucketBy(10,"keyword")
      .sortBy("keyword_catalog_pv_rate")
      .saveAsTable("keyword_bucket")

    val df10 = spark.read.table("keyword_bucket")
//    df10.limit(10).show()
//    df10.printSchema()

    /**
      * Schema Merging(模式合并)
      * parquet支持模式演进，用户可以从simple
      * schema，根据需要逐渐向schema添加更多的columns
      * 读取parquet文件，将data source optinon(数据源选项)
      * 设置为true或将global SQL option(全局SQL选项)spark.sql
      * .parquet.mergeSchema设置为true
      */
    val squareDF = spark.sparkContext
      .makeRDD(1 to 5)
      .map(i =>(i, i * i))
      .toDF("value","square")
    squareDF.write.mode(SaveMode.Overwrite).parquet("data/test_table/key=1")
    val cubeDF = spark.sparkContext
      .makeRDD(6 to 10)
      .map(i=>(i, i * i * i))
      .toDF("value","cube")
    cubeDF.write.mode(SaveMode.Overwrite).parquet("data/test_table/key=2")

    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
//    mergedDF.printSchema()
    /**
      * 创建hive表，无法指定存储处理，可以在hive端创建
      * 示例1：create table keywords(keyword string,
      * catalog_name string) using hive options
      * (fileFormat 'textFile', filedDelim ',')
      * 示例2：create table keywords(keyword string,
      * catalog_name string) row format delimited fields
      * terminated by ','
      */
    spark.sql("drop table keywords")
    spark.sql("create table if not exists keywords" +
      "(keyword String,app_id Int,catalog_name String," +
      "keyword_catalog_pv Double,keyword_catalog_pv_rate " +
      "Double) row format delimited fields terminated by ','")
    spark.sql("load data local inpath " +
      "'/opt/data/keyword_catalog_day.csv' into table " +
      "keywords")
//    spark.sql("select * from keywords").show()
    /**
      * jdbc连接其他数据库
      * 两种方式：
      * (1)spark.read.format(..).options(..).load()
      * (2)spark.read.jdbc(..)
      * numPartitions:读写并行度的最大分区数
      * fetchszie:(读)确定每次数据往返传递行数
      * batchsize:(写)确定每次数据往返传递行数
      * 注意：mysql8.0.11驱动由com.mysql.jdbc.Driver改为com.mysql.cj.jdbc.Driver
       */

    val url = "jdbc:mysql://lee:3306/meiyou?createDatabaseIfNotExist=true&useSSL=false"
    val tableName = "user_pro"
    val predictes = Array("age < 20","age >= 20 and age <26", "age >=26")
    val prop = new java.util.Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","EOSspark123")
    prop.setProperty("driver","com.mysql.cj.jdbc.Driver")
    val df11 = spark.read.jdbc(url,tableName,predictes,prop)

//    val df11 = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://lee:3306/meiyou?createDatabaseIfNotExist=true&useSSL=false")
//      .option("driver","com.mysql.cj.jdbc.Driver")
//      .option("dbtable","user_pro")
//      .option("user","root")
//      .option("password","EOSspark123")
//      .load()
//    df11.show()
//    df11.printSchema()

    /**
      * 按照省份和星座分组后进行身高排序
      */
    df11.createOrReplaceTempView("user_pro")
    val df12 = spark.sql("select * from (select ceil,qq,birthday,height,star_sign,weight,province,recipient,row_number() over (partition by star_sign,province order by height desc) rank from user_pro) sub_user where sub_user.rank <= 3")
//    df12.show()
    /**
      * UDF函数：表单行转换
      * 身高，体重=>BMI指数=体重(kg)/身高(m)的平方
      */
    spark.udf.register("get_bmi",(weight:Double, height:Double)=>{
      val ht = if(height > 100) height/100 else height
      weight/Math.pow(ht, 2)
    })
    val df13 = spark.sql("select *,get_bmi(weight, " +
      "height) bmi from user_pro")
//    df13.show()
//    df13.printSchema()
    /**
      * UDAF函数
      * 弱类型:需要继承UserDefineAggregateFunction并实现相关方法，比较灵活
      * 强类型：需要继承Aggregator并实现相关方法,适合json加载
      * https://blog.csdn.net/liangzelei/article/details/80608302
      */
    spark.udf.register("get_average",new AverageAge1)
    val df14 = spark.sql("select province,get_average" +
      "(age) age1 from user_pro group by province")
//    df14.show()
//    df14.printSchema()

    val df15 = spark.sql("select * from user_pro").as[UserPor]
    val aver = new Average2().toColumn.name("age2")
    val df16 = df15.select(aver)
//    df16.show()
//    df16.printSchema()

  }

  case class PageView(umengmac:String, os:String,
                      ot:String, imsi:String, ua:String,
                      mac:String, openudid:String,
                      manufacturer:String,
                      sdkversion:String, mac_real:String,
                      serial:String, buildv:String,
                      imei:String, osversion: String,
                      umengid:String, romversion:String,
                      androidid:String, apn:String,
                      uid: String, source:String,
                      channelid:String, v:String)
  case class Keyword(keyword:String, app_id:Int,
                     catalog_name:String,
                     keyword_catalog_pv:Double,
                     keyword_catalog_pv_rate:Double)
  case class UserPor(uid:Int,ceil:String,qq:String,
                     age:Int,birthday:String,
                     height:Double,is_married:Int,
                     duration_of_menstruation:Int,
                     menstrual_cycle:Int,star_sign:Int,
  weight:Double,province:String,city:String,
                     recipient:String,recip_ceil:String)
  case class Aver(var sum: Int, var count: Int)

  /**
    * UDAF弱类型
    */
  class AverageAge1 extends UserDefinedAggregateFunction{
//    输入数据
    override def inputSchema: StructType = StructType(StructField("age",IntegerType)::Nil)

//    每个分区共享变量
    override def bufferSchema: StructType = StructType(StructField("sum", IntegerType) :: StructField("count", IntegerType) :: Nil)

//    UDAF的输出类型
    override def dataType: DataType = DoubleType

//    表示如果有相同的输入是否存在相同的输出，如果是则true
    override def deterministic: Boolean = true

//    初始化每个分区中的 共享变量
    override def initialize
    (buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0
      buffer(1) = 0
    }

//    每一个分区中的每一条数据  聚合的时候需要调用该方法
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      // 获取这一行中的年龄，然后将年龄加入到sum中
      buffer(0) = buffer.getInt(0) + input.getInt(0)
      // 将年龄的个数加1
      buffer(1) = buffer.getInt(1) + 1
    }

//    将每一个分区的输出合并，形成最后的数据
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      // 合并总的年龄
      buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
      // 合并总的年龄个数
      buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
    }

//    给出计算结果
    override def evaluate(buffer: Row): Any = {
      // 取出总的年龄 / 总年龄个数
      buffer.getInt(0).toDouble / buffer.getInt(1)
    }
  }

  /**
    * UDAF强类型
    */
  class Average2 extends Aggregator[UserPor, Aver, Double]{
//    初始化方法 初始化每一个分区中的 共享变量
    override def zero: Aver = Aver(0, 0)

//    每一个分区中的每一条数据聚合的时候需要调用该方法
    override def reduce(b: Aver, a: UserPor): Aver = {
      b.sum = b.sum + a.age
      b.count = b.count + 1
      b
    }

//    将每一个分区的输出 合并 形成最后的数据
    override def merge(b1: Aver, b2: Aver): Aver = {
      b1.sum = b1.sum + b2.sum
      b1.count = b1.count + b2.count
      b1
    }

//    给出计算结果
    override def finish(reduction: Aver): Double = {
      reduction.sum.toDouble / reduction.count
    }

//    主要用于对共享变量进行编码
    override def bufferEncoder: Encoder[Aver] = Encoders.product

//    主要用于将输出进行编码
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

}
