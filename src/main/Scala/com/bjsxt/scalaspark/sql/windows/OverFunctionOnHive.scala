package com.bjsxt.scalaspark.sql.windows


import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * over 窗口函数
  * row_number() over(partition by xx order by xx) as rank
  * rank 在每个分组内从1开始
  */
object OverFunctionOnHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("over").enableHiveSupport().getOrCreate()
    spark.sql("use spark")
    spark.sql("create table if not exists sales (riqi string,leibie string,jine Int) " + "row format delimited fields terminated by '\t'")
    spark.sql("load data local inpath '/root/test/sales' into table sales")

    val result = spark.sql(
      "select"
                +" riqi,leibie,jine "
              + "from ("
                  + "select "
                      +"riqi,leibie,jine," + "row_number() over (partition by leibie order by jine desc) rank "
                  + "from sales) t "
              + "where t.rank<=3")
    result.write.mode(SaveMode.Append).saveAsTable("salesResult")
    result.show(100)
  }
}
