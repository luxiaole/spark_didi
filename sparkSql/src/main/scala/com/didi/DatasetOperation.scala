package com.didi


import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.LoggerFactory



object DatasetOperation {
  val logger = LoggerFactory.getLogger(DatasetOperation.getClass)
  val spark = SparkSession
    .builder()
    .master("local[2]")
    .config("spark.some.config.option", "some-value")
    .appName("DatasetOperation")
    .getOrCreate()

  import spark.implicits._
  case class Person(name:String, age:Long)
  def main(args: Array[String]): Unit = {

    val caseClassDS: Dataset[Person] = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
    val path = "D:\\workspace\\spark_didi\\sparkSql\\src\\main\\resources\\person.json"
    val peopleDS: Dataset[Person] = spark.read.json(path).as[Person]
    peopleDS.show()
  }
}
