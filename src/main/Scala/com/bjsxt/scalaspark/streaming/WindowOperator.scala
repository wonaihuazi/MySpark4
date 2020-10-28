package com.bjsxt.scalaspark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.SparkConf

/**
  * SparkStreaming 窗口操作
  * reduceByKeyAndWindow
  * 每隔窗口滑动间隔时间 计算 窗口长度内的数据，按照指定的方式处理
  */
object WindowOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("windowOperator")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop103",9999)
    val words: DStream[String] = lines.flatMap(line=>{line.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(word=>{(word,1)})

    /**
      * 窗口操作普通的机制
      */
   // val windowResult: DStream[(String, Int)] =
    //  pairWords.reduceByKeyAndWindow((v1:Int, v2:Int)=>{v1+v2},Durations.seconds(15),Durations.seconds(5))

    /**
      * 窗口操作优化的机制
      */
    ssc.checkpoint("./data/streamingCheckpoint")
    val windowResult: DStream[(String, Int)] = pairWords.reduceByKeyAndWindow((v1:Int, v2:Int)=>{v1+v2}, (v1:Int, v2:Int)=>{v1-v2},Durations.seconds(15),Durations.seconds(5))

    windowResult.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
