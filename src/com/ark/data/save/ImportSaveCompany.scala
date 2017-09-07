package com.ark.data.save

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types._
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.SaveMode
import java.util.Properties

object ImportSaveCompany {
  val conf = new SparkConf().setAppName("DataImport").setMaster("local").set("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]): Unit = {
    
    
    if(args == null){
      System.exit(0)
    }
    
    
    def getProperties(): Properties = {
  		  var properties = new Properties();
  		  properties.put("user", "root")
  		  properties.put("password", "root")
  		  properties.put("driver", "com.mysql.jdbc.Driver")
  		  properties
    }
    
    val prop = getProperties
    val dirName = args(0)
    val sqlContext = new SQLContext(sc)
    val origDF = sqlContext.read.json(dirName)
    
    
    val rows = origDF.rdd.zipWithUniqueId().map {
      case (r: Row, id: Long) => Row.fromSeq(r.toSeq :+ id)
    }
    val df = sqlContext.createDataFrame(rows, StructType(origDF.schema.fields :+ StructField("company_id", LongType, false)))
    
    df.createTempView("company")
    df.write.mode(SaveMode.Overwrite).json("/root/data/company.json")
    
    
    val reportDF = sqlContext.sql("select company_id, ent.reports as report from company where ent.reports is not null")
  	val reportRDD = reportDF.rdd
  	val reportEntity = reportRDD.flatMap{
  	  x => {
  	    //先把report从表中取出来，此时的结构是一个company_id对应多个row
  	    val rows = x.getAs[WrappedArray[Row]]("report")
  	    val compId: Long = x.getAs[Long]("company_id")
  	    
  	    var list = List[Row]()
  	    //对rows做循环，每个row中都是一条Report实体
  	    rows.foreach { row => {
  	      val cols = row.toSeq
  	      val newRow = Row.fromSeq(compId+:cols)
  	      list = newRow+:list
  	    } }
  	    list.toArray[Row]
  	  }
  	}.zipWithUniqueId().map {
      case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
    }
  	
  	val fields = reportDF.schema.fields
  	val second = fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
  	val reportSchema = StructType(StructField("report_id", LongType, false) +:StructField("company_id", LongType, false)+: second.fields)
  	val reportEntityDF = sqlContext.createDataFrame(reportEntity, reportSchema)
    reportEntityDF.write.mode(SaveMode.Overwrite).json("/root/data/report.json")
    
  }
}