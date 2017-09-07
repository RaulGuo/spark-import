package com.ark.streaming

import org.apache.spark.streaming._

class FirstStream {
  import org.apache.spark._
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    val lines = ssc.socketTextStream("localhost", 9999)
    
    val words = lines.flatMap { x => x.split(" ") }
    
    val count = words.map { x => (x, 1) }.reduceByKey(_+_)
    
    count.print()
    
    ssc.start()

    ssc.awaitTermination()
    
  }
}