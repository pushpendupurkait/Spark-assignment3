package com.knoldus

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreaming extends App{

  val conf: SparkConf = new SparkConf().setAppName("Spark-Streaming").setMaster("local[4]")
  val sc: SparkContext = new SparkContext(conf)
  val streamingContext = new StreamingContext(sc, Seconds(5))
  val host = "localhost"
  val port = 10000

  val customReceiverStream = streamingContext.receiverStream(new CustomReceiver(host, port))
  val words = customReceiverStream.flatMap(_.split(" "))
  println(words)

  streamingContext.start()
  streamingContext.awaitTermination()
}
