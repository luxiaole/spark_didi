package com.didi

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* 常见的RDD操作
* 主要包含创建，转换，行动操作
*
* */
object RDDOperation {
  def main(args: Array[String]): Unit = {
    //创建sc
    val conf = new SparkConf().setAppName("wc").setMaster("local[2]")
    val sc = new SparkContext(conf)


    //RDD的创建方式大概可以分为三种：
    //（1）、从集合中创建RDD；
    //parallelize方法创建
    var source  = sc.parallelize(1 to 10)
    val listRDD: RDD[Int] = sc.parallelize(List(1,2,3))
    //makeRDD方法创建
    val source2: RDD[Int] = sc.makeRDD(List(1,2,3))
    //（2）、从外部存储创建RDD；
    val aaaRDD: RDD[String] = sc.textFile("hdfs://master:8022/aaa.txt/aaa.txt")
    //（3）、从其他RDD创建。
    val wordRDD: RDD[String] = aaaRDD.flatMap(_.split(" "))

    /*
    * 常见的Transformation
    * 转换算子
    * */

    //map(func):返回一个新的RDD，该RDD由每一个输入元素经过func函数转换后组成
    val rdd1: RDD[Int] = sc.parallelize(1 to 10)
    val rdd2: RDD[Int] = rdd1.map(_*2)
    rdd2.collect().foreach(println)

    //filter(func):返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成
    var sourceFilter = sc.parallelize(Array("xiaoming","xiaojiang","xiaohe","dazhi"))
    val filter: RDD[String] = sourceFilter.filter(_.contains("xiao"))
    filter.collect().foreach(println)

    //flatMap(func):类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素）
    val sourceFlat = sc.parallelize(1 to 5)
    val flatMap: RDD[Int] = sourceFlat.flatMap(1 to _)
    flatMap.collect().foreach(println)

    //mapPartitions(func):类似于map，但独立地在RDD的每一个分片上运行，因此在类型为T的RDD上运行时，
    // func的函数类型必须是Iterator[T] => Iterator[U]。假设有N个元素，有M个分区，那么map的函数的
    // 将被调用N次,而mapPartitions被调用M次,一个函数一次处理所有分区
    val rdd = sc.parallelize(List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female")))
    def partitionsFun(iter : Iterator[(String,String)]) : Iterator[String] = {
      var woman = List[String]()
      while (iter.hasNext){
        val next = iter.next()
        next match {
          case (_,"female") => woman = next._1 :: woman
          case _ =>
        }
      }
      woman.iterator
    }
    val result: RDD[String] = rdd.mapPartitions(partitionsFun)
    result.collect().foreach(println)

    //mapPartitionsWithIndex(func):类似于mapPartitions，但func带有一个整数参数表示分片的索引值，因此在类型为T
    // 的RDD上运行时，func的函数类型必须是(Int, Interator[T]) => Iterator[U]
    def partitionsFunIdx(index : Int, iter : Iterator[(String,String)]) : Iterator[String] = {
      var woman = List[String]()
      while (iter.hasNext){
        val next = iter.next()
        next match {
          case (_,"female") => woman = "["+index+"]"+next._1 :: woman
          case _ =>
        }
      }
      woman.iterator
    }
    val result1: RDD[String] = rdd.mapPartitionsWithIndex(partitionsFunIdx)
    result1.collect().foreach(println)

    //sample(withReplacement, fraction, seed):以指定的随机种子随机抽样出数量为fraction的数据，withReplacement
    // 表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。例子从RDD
    // 中随机且有放回的抽出50%的数据，随机种子值为3（即可能以1 2 3的其中一个起始值）
    val rdd3: RDD[Int] = sc.parallelize(1 to 10)
    val samRDD: RDD[Int] = rdd3.sample(false,0.5,5)
    samRDD.collect().foreach(println)

    //takeSample:和Sample的区别是：takeSample返回的是最终的结果集合。
    val ints: Array[Int] = rdd3.takeSample(true,2,2)
    ints.foreach(println)

    //union(otherDataset):对源RDD和参数RDD求并集后返回一个新的RDD
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    rdd4.collect().foreach(println)

    //intersection(otherDataset):对源RDD和参数RDD求交集后返回一个新的RDD
    val rdd5: RDD[Int] = rdd1.intersection(rdd2)
    rdd5.collect().foreach(println)

    //distinct([numTasks])):对源RDD进行去重后返回一个新的RDD. 默认情况下，只有8个并行任务来操作，但是可以
    // 传入一个可选的numTasks参数改变它。
    val testRdd = sc.parallelize(List(1,2,1,5,2,9,6,1))
    val distinctRDD: RDD[Int] = testRdd.distinct()
    distinctRDD.collect().foreach(println)

    //partitionBy:对RDD进行分区操作，如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区， 
    //否则会生成ShuffleRDD. 
    val rdd6 = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
    println(rdd6.partitions.size)
    val partitionRDD: RDD[(Int, String)] = rdd6.partitionBy( new org.apache.spark.HashPartitioner(2))
    println(partitionRDD.partitions.size)

    //reduceByKey(func, [numTasks]):在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，
    // 将相同key的值聚合到一起，reduce任务的个数可以通过第二个可选的参数来设置
    val rdd7: RDD[(String, Int)] = sc.parallelize(List(("female",1),("male",5),("female",5),("male",2)))
    val reduceRDD: RDD[(String, Int)] = rdd7.reduceByKey((x,y) => x+y)
    reduceRDD.collect().foreach(println)

    //groupByKey:groupByKey也是对每个key进行操作，但只生成一个sequence。
    val words = Array("one", "two", "two", "three", "three", "three")
    val wordPairsRDD : RDD[(String, Int)] = sc.parallelize(words).map(word => (word, 1))
    val groupRDD: RDD[(String, Iterable[Int])] = wordPairsRDD.groupByKey()
    val result2: RDD[(String, Int)] = groupRDD.map(t => (t._1,t._2.sum))
    result2.collect().foreach(println)

    //combineByKey[C](createCombiner: V => C,mergeValue: (C, V) => C,mergeCombiners: (C, C) => C)
    /*对相同K，把V合并成一个集合.
createCombiner: combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，
要么就 和之前的某个元素的键相同。如果这是一个新的元素,combineByKey() 会使用一个叫作
createCombiner() 的函数来创建那个键对应的累加器的初始值
mergeValue: 如果这是一个在处理当前分区之前已经遇到的键， 它会使用 mergeValue() 方法将该键的累加器对应的当前值与这个新的值进行合并
mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。如果有两个或者更多的分区都有对应同一个键的累加器， 就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。*/
    val scores = Array(("Fred", 88), ("Fred", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98))
    val input: RDD[(String, Int)] = sc.parallelize(scores)
    val combine: RDD[(String, (Int, Int))] = input.combineByKey(
      (v)=>(v,1),
      (acc:(Int,Int),v)=>(acc._1+v,acc._2+1),
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))
    //Fred,seq(88,95,91)  Wilma,seq(93,95,98)
    //(88,1),(95,1),(91,1)
    val result3 = combine.map{
      case (key,value) => (key,value._1/value._2.toDouble)}
    result3.collect().foreach(println)
    //aggregateByKey(zeroValue:U,[partitioner: Partitioner]) (seqOp: (U, V) => U,combOp: (U, U) => U):
    /* 在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，进行计算，
     返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给combine函数进行
     计算（先将前两个value进行计算，将返回结果和下一个value传给combine函数，以此类推），将key与计算结果
     作为一个新的kv对输出。 seqOp函数用于在每一个分区中用初始值逐步迭代value，combOp函数用于合并每个分
     区中的结果*/
    val rdd8 = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    val agg: RDD[(Int, Int)] = rdd8.aggregateByKey(0)(math.max(_,_),_+_)
    agg.collect().foreach(println)//Array((3,8), (1,7), (2,3))
    println(agg.partitions.size)//3

    val rdd9 = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),1)
    val agg2: RDD[(Int, Int)] = rdd9.aggregateByKey(0)(math.max(_,_),_+_)
    agg2.collect().foreach(println)//Array((1,4), (3,8), (2,3))

    //foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]:aggregateByKey的简化操作，seqop和combop相同
    val agg3 = rdd8.foldByKey(0)(_+_)
    agg3.collect().foreach(println)//Array((3,14), (1,9), (2,3))

    //sortByKey([ascending], [numTasks]):在一个(K,V)的RDD上调用，K必须实现Ordered接口，
    // 返回一个按照key进行排序的(K,V)的RDD
    val rdd10 = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    val tuples: Array[(Int, String)] = rdd10.sortByKey(true).collect()//按key升序
    tuples.foreach(println)//Array((1,dd), (2,bb), (3,aa), (6,cc))

    //sortBy(func,[ascending], [numTasks])：与sortByKey类似，但是更灵活,可以用func先对数据进行处理，
    // 按照处理后的数据比较结果排序。
    val rdd11 = sc.parallelize(List(1,2,3,4))
    rdd11.sortBy(x => x).collect()//Array(1, 2, 3, 4)
    rdd11.sortBy(x => x%3).collect()//Array(3, 4, 1, 2)

    //join(otherDataset, [numTasks]):在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的
    // 所有元素对在一起的(K,(V,W))的RDD
    val rdd12: RDD[(Int, String)] = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd13: RDD[(Int, Int)] = sc.parallelize(Array((1,4),(2,5),(3,6)))
    val result4: RDD[(Int, (String, Int))] = rdd12.join(rdd13)
    result4.collect().foreach(println)// Array((1,(a,4)), (2,(b,5)), (3,(c,6)))

    //cogroup(otherDataset, [numTasks]):在类型为(K,V)和(K,W)的RDD上调用，返回一个
    // (K,(Iterable<V>,Iterable<W>))类型的RDD
    val tuples2: Array[(Int, (Iterable[String], Iterable[Int]))] = rdd12.cogroup(rdd13).collect()

    //cartesian(otherDataset):笛卡儿积
    val rdd14 = sc.parallelize(1 to 3)
    val rdd15 = sc.parallelize(2 to 5)
    rdd14.cartesian(rdd15).collect()//Array((1,2), (1,3), (1,4), (1,5), (2,2), (2,3), (2,4), (2,5), (3,2), (3,3), (3,4), (3,5))

    //pipe(command, [envVars]):对于每个分区，都执行一个perl或者shell脚本，返回输出的RDD
    /* val rdd16 = sc.parallelize(List("hi","Hello","how","are","you"),1)
     val strings: Array[String] = rdd16.pipe("hdfs://master:8022/pipe.sh").collect()
     println(strings)*/

    //coalesce(numPartitions):缩减分区数，用于大数据集过滤后，提高小数据集的执行效率
    val rdd17 = sc.parallelize(1 to 16,4)
    val coalesceRDD = rdd.coalesce(2)

    //repartition(numPartitions):根据分区数，从新通过网络随机洗牌所有数据。
    //repartitionAndSortWithinPartitions(partitioner):repartitionAndSortWithinPartitions在给定的
    // partitioner内部进行排序，性能比repartition要高。

    //glom:将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
    val rdd18 = sc.parallelize(1 to 16,4)
    rdd18.glom().collect()
    //Array[Array[Int]] = Array(Array(1, 2, 3, 4), Array(5, 6, 7, 8), Array(9, 10, 11, 12), Array(13, 14, 15, 16))

    //mapValues:针对于(K,V)形式的类型只对V进行操作
    val rdd19 = sc.parallelize(Array((1,"a"),(1,"d"),(2,"b"),(3,"c")))
    rdd19.mapValues(_+"kkk")

    //subtract:计算差的一种函数去除两个RDD中相同的元素，不同的RDD将保留下来
    val rdd20 = sc.parallelize(3 to 8)
    val rdd21 = sc.parallelize(1 to 5)
    rdd20.subtract(rdd21)//(6,7,8)

    //------------------------------------------常用转换算子结束------------------------------------------------------

    /*
    * 常用的Action算子
    * */

    //reduce(func)：通过func函数聚集RDD中的所有元素，这个功能必须是可交换且可并联的
    val rdd22 = sc.makeRDD(1 to 10,2)
    val i: Int = rdd1.reduce(_+_)
    println(i)//55

    //collect():在驱动程序中，以数组的形式返回数据集的所有元素

    //count():返回RDD的元素个数

    //first():返回RDD的第一个元素（类似于take(1)）

    //take(n):返回一个由数据集的前n个元素组成的数组

    //takeSample(withReplacement,num, [seed]):返回一个数组，该数组由从数据集中随机
    // 采样的num个元素组成，可以选择是否用随机数替换不足的部分，seed用于指定随机数生成器种子
    rdd22.takeSample(true,5,2)//是否放回的抽样

    //takeOrdered(n)：返回排序后的前几个元素

    //aggregate (zeroValue: U)(seqOp: (U, T) ⇒ U, combOp: (U, U) ⇒ U)：aggregate函数将每个分区里
    // 面的元素通过seqOp和初始值进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行
    // combine操作。这个函数最终返回的类型不需要和RDD中元素类型一致。

    //fold(num)(func)：折叠操作，aggregate的简化操作，seqop和combop一样

    //saveAsTextFile(path)：将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件
    // 系统，对于每个元素，Spark将会调用toString方法，将它装换为文件中的文本
    //saveAsObjectFile(path):用于将RDD中的元素序列化成对象，存储到文件中。
    //saveAsSequenceFile(path):将数据集中的元素以Hadoop sequencefile的格式保存到指定的目录下，
    // 可以使HDFS或者其他Hadoop支持的文件系统

    //countByKey():针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。
    //countByValue():
    val rdd23 = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    val intToLong: collection.Map[Int, Long] = rdd23.countByKey()//Map(3 -> 2, 1 -> 3, 2 -> 1)

    //foreach(func):在数据集的每一个元素上，运行函数func进行更新
    rdd23.foreach(println)

    //--------------------------------------数值RDD的统计操作-------------------------------------------------

    /*方法：count（）  RDD中的元素个数
            mean（）  平均值
            sum（）   总和
            max（）   最大值
            min（）   最小值
            variance（）  方差
            sampleVariance（）  采样中计算方差
            stdev（）   标准差
            sampleStdev（）   采样标准差
    */

    //persist()和cache()方法进行rdd缓存，供后面重用，默认情况下 persist()
    // 会把数据以序列化的形式缓存在 JVM 的堆空间中，也可设置缓存级别
    // cache.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)


    /*
    * 键值对RDD的操作
    * Spark 为包含键值对类型的 RDD 提供了一些专有的操作 在PairRDDFunctions专门进行了定义。这些 RDD 被称为 pair RDD
    * ctrl+n 搜索PairRDDFunctions查看
    * */
    //----------------------------------------常见的键值对RDD操作-----------------------------------------------

    /*
    *   转化操作列表
    * */

    /*

    rdd.reduceByKey()                   rdd.subtractByKey(rdd1)
    rdd.groupByKey()                      rdd.join(rdd1)
    rdd.combineByKey()                    rdd.rightOutJoin(rdd1),leftOutJoin()
    rdd.flatMapValues()                   rdd.cogroup(rdd1)
    rdd.keys()
    rdd.sortByKey()*/

    /*
    *   1.聚合操作
    *   (1).reduceByKey():接收一个函数，并使用该函数对值进行合并。
    *   (2).foldByKey():与 fold() 相当类似;它们都使用一个与 RDD 和合并函数中的数据类型相 同的零值作为初始值。
    *   与 fold() 一样，foldByKey() 操作所使用的合并函数对零值与另一 个元素进行合并，结果仍为该元素。
    *   (3).combineByKey():基于键进行聚合的函数
    * */

    /*
    *   2.数据分组
    *   groupBy():可以用于未成对的数据上，也可以根据除键相同以外的条件进行分组。它可以 接收一个函数，对源
     *   RDD 中的每个元素使用该函数，将返回结果作为键再进行分组
    * */

    /*
    *   3.连接操作
    *   join操作
    * */

    /*
    *   4.排序操作
    *   sortByKey()：函数接收一个叫作 ascending 的参数，表示我们是否想要让结果按升序排序(默认值为 true)。
    * */


    /*
    *   行动操作
    *   countByKey():对每个键对应的元素分别计数
    *
    *   collectAsMap（）：将结果以映射表的形式返回
    * */

    /*
    *   分区操作
    *   rdd.partitioner():获取分区方式
    * */

    //------------------------------------数据输出------------------------------------------

    /*
    * spark连接mysql，hbase
    * */

    //Mysql读取:支持通过Java JDBC访问关系型数据库。需要通过JdbcRDD进行
    val jdbcRDD = new org.apache.spark.rdd.JdbcRDD(sc,
      () => {
        Class.forName ("com.mysql.jdbc.Driver").newInstance()
        java.sql.DriverManager.getConnection ("jdbc:mysql://master01:3306/rdd", "root", "hive")
      },"select * from rddtable where id >= ? and id <= ?;",
    1,
    10,
    1,
    r => (r.getInt(1), r.getString(2)))

      //Mysql写入
      val data = sc.parallelize(List("Female", "Male","Female"))
    data.foreachPartition(insertData)


    //HBase读取：Spark 可以通过Hadoop输入格式访问HBase。这个输入格式会返回键值对数据，
    // 其中键的类型为org. apache.hadoop.hbase.io.ImmutableBytesWritable，而值的类型
    // 为org.apache.hadoop.hbase.client.Result
   /* val conf = HBaseConfiguration.create()
    //HBase中的表名
    conf.set(TableInputFormat.INPUT_TABLE, "fruit")

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val count = hBaseRDD.count()
    println("hBaseRDD RDD Count:"+ count)
    hBaseRDD.cache()
    hBaseRDD.foreach {
      case (_, result) =>
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("info".getBytes, "name".getBytes))
        val color = Bytes.toString(result.getValue("info".getBytes, "color".getBytes))
        println("Row key:" + key + " Name:" + name + " Color:" + color)
    }
    sc.stop()*/



  }


  def insertData(iterator: Iterator[String]): Unit = {
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://master01:3306/rdd", "root", "hive")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into rddtable(name) values (?)")
      ps.setString(1, data)
      ps.executeUpdate()
    })
  }

}
