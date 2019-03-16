package com.didi

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object TestSparkSql {
  val logger = LoggerFactory.getLogger(TestSparkSql.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local[2]")
      .getOrCreate()

 /*   val conf = new SparkConf().setAppName("wc").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.textFile("")

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

     val df: DataFrame = spark.read.json("")

    // Displays the content of the DataFrame to stdout
    df.show()

    df.filter($"age" > 21).show()

    df.createOrReplaceTempView("persons")

    spark.sql("SELECT * FROM persons where age > 21").show()

    spark.stop()*/
  }


}
