package com.ark.data.test

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


object ImportCompanyTest {
  val conf = new SparkConf().setAppName("DataImport").setMaster("local").set("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse")
  val sc = new SparkContext(conf)
  val dbUrl = "jdbc:mysql://180.76.153.111:3306/sample?autoReconnect=true"
  
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
    val origDF = sqlContext.read.json(dirName)//"F:\\spark-import\\sample"
//    val origDF = sqlContext.read.json("F:\\spark-import\\sample")
    val rows = origDF.rdd.zipWithUniqueId().map {
      case (r: Row, id: Long) => Row.fromSeq(r.toSeq :+ id)
    }
    val df = sqlContext.createDataFrame(rows, StructType(origDF.schema.fields :+ StructField("company_id", LongType, false)))
    
    df.createTempView("company")
    
    val mortgagecollateralDF = sqlContext.sql("select company_id, bus.mortgages.collateral from company where bus.mortgages.collateral is not null")
    val mortgagecollateralRDD = mortgagecollateralDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[WrappedArray[Row]]]("collateral")
        val compId: Long = x.getAs[Long]("company_id")
        var list = List[Row]()
        rows.foreach { rowArray => {
          if(rowArray != null){
          rowArray.foreach { row => {
            val cols = row.toSeq
            val newRow = Row.fromSeq(compId+:cols)
            list = list:+newRow
        }}}}}
        list.toArray[Row]
      }
    }.zipWithUniqueId().map {
      case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
    }
    val mortgagecollateralStructType = mortgagecollateralDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val mortgagecollateralSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: mortgagecollateralStructType.fields)
    val flatmortgagecollateralDF = sqlContext.createDataFrame(mortgagecollateralRDD, mortgagecollateralSchema).withColumnRenamed("type", "type_name")
    flatmortgagecollateralDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_mortgage_debt_secure", prop)
  }
}