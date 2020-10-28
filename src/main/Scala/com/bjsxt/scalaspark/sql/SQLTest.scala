package com.bjsxt.scalaspark.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

case class Person(name: String, age: Long)

object SQLTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val peopleDF = spark.sparkContext
    .textFile("./data/people.txt")
    .map(_.split(","))
    .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    .toDF()

    peopleDF.createOrReplaceTempView("people")

    val frame: DataFrame = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    val teenagersDF = frame

    teenagersDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
//    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

//     row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    val value: Dataset[Map[String, Any]] = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age")))
    value.count()
    // Array(Map("name" -> "Justin", "age" -> 19))
  }
}
