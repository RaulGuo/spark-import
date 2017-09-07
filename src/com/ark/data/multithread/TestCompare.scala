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

object TestCompare {
  var prop = new Properties();
  prop.put("user", "root")
  prop.put("password", "root")
  prop.put("driver", "com.mysql.jdbc.Driver")
  
  val dbUrl = "jdbc:mysql://180.76.153.111:3306/multithread?autoReconnect=true"
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataImport").config("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse").enableHiveSupport().getOrCreate()
    val reportEntityDF = spark.read.json("/root/data/report.json")
    val start = System.currentTimeMillis()
    reportEntityDF.createTempView("report")
    
    val list = 1 :: 2 :: 3 :: 4 :: Nil;
    
    
    try{
      val reportbranchDF = spark.sql("select report_id, branchs from report where branchs is not null")
      val reportbranchRDD = reportbranchDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("branchs")
          val reportId: Long = x.getAs[Long]("report_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(reportId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }.zipWithUniqueId().map {
        case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
      }
      val reportbranchStructType = reportbranchDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val reportbranchSchema = StructType(StructField("id", LongType, false) +: StructField("report_id", LongType, false) +: reportbranchStructType.fields)
      val flatreportbranchDF = spark.createDataFrame(reportbranchRDD, reportbranchSchema)
      flatreportbranchDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_branch", prop)
      
      val licenceDF = spark.sql("select report_id, licenses from report where licenses is not null")
      val licenceRDD = licenceDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("licenses")
          val reportId: Long = x.getAs[Long]("report_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(reportId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }.zipWithUniqueId().map {
        case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
      }
      val licenceStructType = licenceDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val licenceSchema = StructType(StructField("id", LongType, false) +: StructField("report_id", LongType, false) +: licenceStructType.fields)
      val flatlicenceDF = spark.createDataFrame(licenceRDD, licenceSchema)
      flatlicenceDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_licence", prop)
      
      
      
      val websiteDF = spark.sql("select report_id, websites from report where websites is not null")
      val websiteRDD = websiteDF.rdd.flatMap{
        x => {
          val rows = x.getAs[WrappedArray[Row]]("websites")
          val reportId: Long = x.getAs[Long]("report_id")
          var list = List[Row]()
          rows.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(reportId+:cols)
            list = list:+newRow
          } }
          list.toArray[Row]
        }
      }.zipWithUniqueId().map {
        case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
      }
      val websiteStructType = websiteDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
      val websiteSchema = StructType(StructField("id", LongType, false) +: StructField("report_id", LongType, false) +: websiteStructType.fields)
      val flatwebsiteDF = spark.createDataFrame(websiteRDD, websiteSchema).withColumnRenamed("type", "type_name")
      flatwebsiteDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_website", prop)
    }finally{
      val end = System.currentTimeMillis()
      println("*************************take notice:********")
      println("import report part data with only one thread cost time:"+(end-start)+"ms")
      println("----------------over-----------------------")
    }
  }
}