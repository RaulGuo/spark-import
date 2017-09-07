package com.ark.data.multithread

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import java.util.Properties
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ArrayType
import java.util.concurrent.TimeUnit

object Test {
  
  def main(args: Array[String]) {
    val str1 = "3500-5000/月"
    println(str1.contains("/月"))
  }
  
}