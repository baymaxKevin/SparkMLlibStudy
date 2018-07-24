package com.sparkStudy.chapter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
  * spark transformation和action
  * transformation：从一个rdd创建一个新的rdd，rdd转换不会触发job
  * 执行，而是构建一个rdd依赖图（执行dag），transformation调用lazy执行
  * action：触发job产生，根据dag图构建job对象，先执行父rdd，再执行当前rdd
  */
object SparkCore {
  def main(args: Array[String]): Unit = {
//    日志级别设置
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.ERROR)
//    创建SparkContext对象，访问spark集群
    val conf = new SparkConf().setAppName("SparkCore")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ar1 = 1 to 10
    val rd = sc.parallelize(ar1,2)

//    外部数据(本地文件系统、hdfs、Cassandra、HBase、Amazon S3等)
    val rdd1 = sc.textFile("/opt/data/git.txt",2)
    /**
      * flatMap：一对多，扁平化
      * map：一对一
      * mapValues：KV pairs中value转换
      * mapParitions：对每个分区数据进行转换，一般用于比较耗时操作，比如链接和断开数据库
      * mapPartitionsWithIndex：mapPartitions
      * 类似，多传入分区值参数，函数：(Int,Iterator<T>)=>Iterator<U>
      */
    val rdd2 = rdd1.flatMap(line=>line.split(" "))
//    一对一，转化为KV pairs
    val rdd3 = rdd2.map(f=>(f,1))
    val rdd4 = rdd3.mapValues(f=>2*f)
    val rdd5 = rdd3.mapPartitionsWithIndex(mapPartitionIndexFunc)
//    filter(boolean)
    val rdd6 = rdd2.filter(f=>f.length>5)
//    sample(withReplacement,fraction,seed)是根据给定随机种子seed，随机抽样出数量为frac的数据。withReplacement：是否放回抽样
    val rdd7 = rdd2.sample(true,0.1)
//    union：两个集合类型一致
    val rdd8 = rdd6.union(rdd7)
//    intersection返回两个集合交集
    val rdd9 = rdd8.intersection(rdd7)
//    distinct返回两个集合去重新集合
    val rdd10 = rdd9.union(rdd9).distinct()

    /**
      * groupByKey(numTasks)是对KV pairs rdd进行分组操作，返回(K,Seq[V])数据集，通过numTasks设置不同并行任务
      * reducByKey(func,[numTasks])是对KV键值对数据分组聚合操作，返回(K,V)的数据集；
      * 区别：数据两大，reduceByKey性能更佳，因为在shuffle输出数据前，会combine每个partition上具有相同key的输出结果，而groupByKey所有键值对都会shuffle
      * aggregateByKey(zeroValue:U)(seqOp:(U,T)=>U,combOp:(U,U)=>U)
      * seqOp操作会聚合各分区中的元素，然后combOp操作把所有分区的聚合结果再次聚合，两个操作的初始值都是zeroValue；
      * seqOp的操作是遍历分区中的所有元素(T)，第一个T跟zeroValue做操作，结果再作为与第二个T做操作的zeroValue，直到遍历完整个分区
      * sortByKey([ascending],[numTasks])对KV pairs数据按照K进行排序，第一个参数ascending，默认，升序，第二个参数，并行任务数量
      */
    val rdd11 = rdd5.groupByKey()
    val rdd12 = rdd3.reduceByKey(_+_,2)
    val rdd13 = rdd2.map(f=>(f,f.length()))
//    查看分区情况
    val rdd14 = rdd13.mapPartitionsWithIndex{
      (partIdx,iter)=>{
        var part_map = scala.collection.mutable.Map[String,List[(String,Int)]]()
        while(iter.hasNext){
          var part_name = "part_" + partIdx
          var elem = iter.next
          if(part_map.contains(part_name)){
            var elems = part_map(part_name)
            elems ::=elem
            part_map(part_name) = elems
          }else{
            part_map(part_name) = List[(String,Int)]{elem}
          }
        }
        part_map.iterator
      }
    }
    val rdd15 = rdd13.aggregateByKey(0)(Math.min(_,_),_+_)
    val rdd16 = rdd3.sortByKey()
//    sortByKey通过重写ordering修改默认排序规则
    implicit val sortByLength = new Ordering[String]{
      override def compare(a:String, b:String) =
        a.length.compare(b.length)
    }
    val rdd17 = rdd3.sortByKey()

    /**
      * join
      * leftOuterJoin
      * RightOuterJoin
      * fullOuterJoin
      */
    val rdd18 = rdd3.reduceByKey(_+_)
    val rdd19 = rdd18.sample(false,0.1,2)
    val rdd20 = rdd18.join(rdd19)
    val rdd21 = rdd18.leftOuterJoin(rdd19)
    val rdd22 = rdd19.rightOuterJoin(rdd18)
    val rd0 = rdd19.fullOuterJoin(rdd18)

    /**
      * ogroup(otherDataset,[numTasks])是将输入数据集(K,V)和另外一个数据集(K,W)聚合一个集合(K,Seq[V],Seq[W]);
      * cartesian(otherDataset)是对数据集T和U做笛卡尔积操作
      */
    val rdd23 = rdd19.cogroup(rdd19)
    val rdd24 = rdd19.cartesian(rdd19)
//    pipe(command,[envVars])是以shell命令处理rdd
    rdd24.pipe("head -n 1").collect()

    /**
      * randomSplit(weights:Array[Double],seed:Long = Utils.random.nextLong):Array[RDD]
      * 对RDD按照权重进行数据分割，第一个参数为分割权重数组，第二个参数为随机种子。
      */
    val rdds = rdd18.randomSplit(Array(0.3,0.7),2)
    val rdd25 = rdds(0)
    val rdd26 = rdds(1)
//    subtract(other:RDD[T]):RDD[T]是对RDD进行减法操作
    val rdd27 = rdd18.subtract(rdd25)

    /**
      * zip[U:ClassTag](other:RDD[U]):RDD[(T,U)]将两个RDD组合成KV pairs RDD，这两个RDD partition数量和元素数量相同
      * zipPartitions将多个RDD按照partition组合成新的RDD ，该函数需要组合的RDD具有相同的分区数，但对元素没有要求
      * zipWithIndex将rdd元素及其索引进行拉链，索引始于0，组合成键值对
      * zipWithUniqueId每个元素对应唯一id值，id不一定和真实元素索引一致，唯一id生成算法:
      * 每个分区第一个元素唯一id为：该分区索引号，每个分区第N个元素唯一ID值为：(前一个元素唯一ID值) + (该RDD总分区数)
      */
    val rd1 = rdd3.map(_._1)
    val rd2 = rdd3.map(_._2)
    val rd3 = rd1.zip(rd2)
//    rd1元素分区
    val rd4 = rd1.mapPartitionsWithIndex{
      (partIdx,iter)=>{
        var part_map = scala.collection.mutable.Map[String,List[String]]()
        while(iter.hasNext){
          var elem = iter.next()
          var part_name = "part_" + partIdx
          if(part_map.contains(part_name)){
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          }else{
            part_map(part_name) = List[String]{elem}
          }
        }
        part_map.iterator
      }
    }
    val rd5 = rd2.mapPartitionsWithIndex{
      (partIdx,iter)=>{
        var part_map = scala.collection.mutable.Map[String,List[Int]]()
        while(iter.hasNext){
          var elem = iter.next()
          var part_name = "part_" + partIdx
          if(part_map.contains(part_name)){
            var elems = part_map(part_name)
            elems ::= elem
            part_map(part_name) = elems
          }else{
            part_map(part_name) = List[Int]{elem}
          }
        }
        part_map.iterator
      }
    }

    val rd6 = rd1.zipPartitions(rd2){
      (iter1,iter2)=>{
        var result = List[String]()
        while(iter1.hasNext && iter2.hasNext) {
          result ::= (iter1.next() + "_" + iter2.next())
        }
        result.iterator
      }
    }
    val rd7 = rd1.zipWithIndex
    val rd8 = rd1.zipWithUniqueId
//    唯一id和索引值并不一定相同验证
    val rd9 = rd7.join(rd8)

    /**
      * repartition(numPartitions:Int):RDD[T]和coalesce(numPartitions:Int,shuffle:Boolean=false):RDD[T]
      * repartition是coalesce接口中shuffle为true简易实现，假设分区前N个分区，分区后M个分区：
      * N<M 利用HashPartitioner函数将数据重新分区为M个，shuffle为true
      * N>M（相差不多） 将N个分区若干分区合并为一个新的分区，最终合并为M个分区，shuffle是指为false，父RDD与子RDD是窄依赖。
      * N>M（相差悬殊）若将shuffle设置为false，父子RDD是窄依赖，处于统一stage并行度不够，为此可以将shuffle设置为true。
      * 分区：HashPartitioner分区(默认)、RangePartitioner分区和自定义分区
      * HashPartitioner分区：对于给定key，计算其hashCode
      * ，并除以分区个数取余，如果余数小于0，则用余数+分区个数，最后返回的值就是这个key所属分区ID
      * RangePartitioner分区：将一定范围内的数映射到某个分区，分解算法实现。
      * 自定义分区：需继承org.apache.spark.Partitioner，实现见下：
      * repartitionAndSortWithinPartition:在repartition
      * 重分区还要进行排序建议直接使用repartitionAndSortWithPartition,
      * 可以一边重分区shuffle操作，一边排序，性能高于先shuffle再sort
      */
    val rd10 = rdd18.repartitionAndSortWithinPartitions(new HashPartitioner(3))
    val rd11 = rdd18.mapPartitionsWithIndex{
      (idx,iter)=>{
        var part_map = scala.collection.mutable.Map[String,List[(String,Int)]]()
        while(iter.hasNext) {
          var elem = iter.next()
          var part_name = "part_" + idx
          if(part_map.contains(part_name)){
            var elems = part_map(part_name)
            elems::=elem
            part_map(part_name) = elems
          }else{
            part_map(part_name) = List[(String,Int)]{elem}
          }
        }
        part_map.iterator
      }
    }
    rd11.collect().foreach(println(_))
    val rd12 = rd10.mapPartitionsWithIndex{
      (idx,iter)=>{
        var part_map = scala.collection.mutable.Map[String,List[(String,Int)]]()
        while(iter.hasNext) {
          var elem = iter.next()
          var part_name = "part_" + idx
          if(part_map.contains(part_name)){
            var elems = part_map(part_name)
            elems::=elem
            part_map(part_name) = elems
          }else{
            part_map(part_name) = List[(String,Int)]{elem}
          }
        }
        part_map.iterator
      }
    }
    rd12.collect().foreach(println(_))
    val rd13 = rdd18.repartitionAndSortWithinPartitions(new MyPartition(3))
    val rd14 = rd13.mapPartitionsWithIndex{
      (idx,iter)=>{
        var part_map = scala.collection.mutable.Map[String,List[(String,Int)]]()
        while(iter.hasNext) {
          var elem = iter.next()
          var part_name = "part_" + idx
          if(part_map.contains(part_name)){
            var elems = part_map(part_name)
            elems::=elem
            part_map(part_name) = elems
          }else{
            part_map(part_name) = List[(String,Int)]{elem}
          }
        }
        part_map.iterator
      }
    }
    rd14.collect().foreach(println(_))
    /**
      * reduce 对集合元素进行聚合
      */
    val r1 = sc.parallelize(1 to 9,3)
    val r2 = r1.reduce(_+_)

    /**
      * collect将数据集中所有元素以array返回
      */
    r1.collect()

    /**
      * count返回数据集元素个数
      */
    r1.count()

    /**
      * first返回第一个元素
      * take(n)返回一个包含数据集中前n个元素数组
      */
    r1.first()
    r1.take(3)

    /**
      * takeSmaple(withReplacement,num,[seed])返回包含随机的num个元素的数组，
      * withReplacement是否抽样放回，num抽样个数，seed是随机种子;
      * takeOrdered(n,[ordering])返回i包含随机n个元素数组，按照升序排列；top是按照降序排列
      */
    r1.takeSample(true, 2, -1)
    r1.takeOrdered(2)

    /**
      * 数据集写道文本文件中
      */
//    r1.saveAsTextFile("/opt/data/file1.txt")

    /**
      * 对于KV pairs类型RDD，返回一个(K,Int)的map，Int为K个数。查看是否存在数据倾斜
      */
    rdd3.countByKey()

    /**
      * foreach对数据集每个元素执行func汉书
      */
  }

  /**
    * 统计KV pairs类型rdd元素分区
    */
  def mapPartitionIndexFunc(ix:Int,iter:Iterator[(String,Int)])
  :Iterator[(Int,(String,Int))] = {
    var res = List[(Int,(String,Int))]()
    while(iter.hasNext){
      var next = iter.next()
      res = res.::(ix , next)
    }
    res.iterator
  }

  class MyPartition(numParts : Int) extends Partitioner {
    override def numPartitions: Int = numParts

    /**
      * 自定义分区算法
      */
    override def getPartition(key: Any) : Int = {
      val code = key.toString.length % numParts
      if(code < 0) {
        code + numParts
      }else {
        code
      }
    }

    override def equals(other :Any):Boolean = other match {
      case mypartition : MyPartition =>
        mypartition.numPartitions == numPartitions
      case _=>
        false
    }

    override def hashCode(): Int = numPartitions
  }
}
