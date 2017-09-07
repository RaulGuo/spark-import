package com.ark.data.i30w

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import scala.collection.mutable.ArrayOps
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.SaveMode
import java.util.Properties

object ImportAicChange {
  val conf = new SparkConf().setAppName("DataImport").setMaster("local").set("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse")
  val sc = new SparkContext(conf)
  val dbUrl = "jdbc:mysql://180.76.153.111:30w/sample?autoReconnect=true"
  
  def main(args: Array[String]): Unit = {
    
    
    def getProperties(): Properties = {
  		  var properties = new Properties();
  		  properties.put("user", "root")
  		  properties.put("password", "root")
  		  properties.put("driver", "com.mysql.jdbc.Driver")
  		  properties
    }
    
    val prop = getProperties
    val sqlContext = new SQLContext(sc)
    
    val df = sqlContext.read.json("/root/data/company.json")
    //
    
    val aicchangeDF = sqlContext.sql("select company_id, aic.changes as change from company where change is not null")
  	val aicchangeRDD = aicchangeDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("change")
        val compId: Long = x.getAs[Long]("company_id")
        var list = List[Row]()
        rows.foreach { row => {
          val cols = row.toSeq
          val newRow = Row.fromSeq(compId+:cols)
          list = list:+newRow
        } }
        list.toArray[Row]
      }
    }.zipWithUniqueId().map {
      case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
    }
    val aicchangeStructType = aicchangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val aicchangeSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: aicchangeStructType.fields)
    val flataicChangeDF = sqlContext.createDataFrame(aicchangeRDD, aicchangeSchema)
    flataicChangeDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_aic_change", prop)
  }
}