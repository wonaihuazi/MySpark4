package com.bjsxt.scalaspark.sql

import java.util.Properties

import org.apache.spark.sql.{ Dataset, Row, SparkSession}

object CreateDataFrameFromMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "MyNewPass4!")

    val result: Dataset[Row] = spark.read.jdbc("jdbc:mysql://192.168.137.103:3306/learn", "(" +
      "select user_id as userId,username,sex,age,registertime from user ) T", properties)

    result.show()
  }
}
