package com.didi

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory


object Rdd2Df {

  val logger = LoggerFactory.getLogger(Rdd2Df.getClass)
  val spark = SparkSession
    .builder()
    .config("spark.some.config.option", "some-value")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._
  def main(args: Array[String]): Unit = {
    val sc = spark.sparkContext
    val peopleRdd: RDD[String] = sc.textFile("D:\\workspace\\spark_didi\\sparkSql\\src\\main\\resources\\people.txt")
    //rdd转换成df
    val rdd1: RDD[(String, Int)] = peopleRdd.map(_.split(",")).map(paras => (paras(0),paras(1).trim().toInt))
    val peopleDf: DataFrame = rdd1.toDF("name","age")

    peopleDf.createOrReplaceTempView("people")
    spark.sql("select * from people").show()
    spark.stop()

    //两种操作风格
    //DSL风格语法
  /*  peopleDf.show()
    peopleDf.printSchema()
    peopleDf.select("name").show()
    peopleDf.filter($"age">21).show()
    peopleDf.groupBy("age").count().show()

    //Sql语法
    peopleDf.createOrReplaceTempView("people")
    val sqlDF = spark.sql("select * from people")
    sqlDF.show*/
  }

}
