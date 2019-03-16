package com.didi

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by wuyufei on 31/07/2017.
  */
object WordCount {

  val logger = LoggerFactory.getLogger(WordCount.getClass)

  def main(args: Array[String]) {
    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setMaster("spark://10.10.30.100:7077").setAppName("WordCount")
      .set("spark.executor.cores", "1")
      .setIfMissing("spark.driver.host", "10.10.30.100")
    // val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")

    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf)
    //使用sc创建RDD并执行相应的transformation和action
    val result = sc.textFile("hdfs://master:8022/person.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _, 1).sortBy(_._2, false)
    //停止sc，结束该任务

    //result.collect().foreach(println _)


    result.saveAsTextFile("hdfs://master:8022/output")


    logger.info("complete!")

    sc.stop()

  }
}