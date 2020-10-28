package com.bjsxt.scalaspark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object TransformBlackList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("transform")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
//    ssc.sparkContext.setLogLevel("Error")
    /**
      * 广播黑名单
      */
    val blackList: Broadcast[List[String]] = ssc.sparkContext.broadcast(List[String]("zhangsan","lisi"))

    /**
      * 从实时数据【"hello zhangsan","hello lisi"】中发现 数据的第二位是黑名单人员，过滤掉
      */
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("c7node5",9999)
    val pairLines: DStream[(String, String)] = lines.map(line=>{(line.split(" ")(1),line)})
    val resultDStream: DStream[String] = pairLines.transform(pairRDD => {
      val filterRDD: RDD[(String, String)] = pairRDD.filter(tp => {
        val nameList: List[String] = blackList.value
        !nameList.contains(tp._1)
      })
      val returnRDD: RDD[String] = filterRDD.map(tp => tp._2)
      returnRDD
    })

    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }
}
