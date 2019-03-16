package com.didi

import org.apache.spark.sql.SparkSession

object HiveOperation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql("SELECT * FROM didi_bigdata_basecar limit 100")
    df.write.csv("hdfs://master:8022/output/sparksqltest")
    df.show()
    spark.stop()
  }

}

