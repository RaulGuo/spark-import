package com.ark.data

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

object ImportReportByJson {
  val conf = new SparkConf().setAppName("DataImport").setMaster("local").set("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse")
  val sc = new SparkContext(conf)
  val dbUrl = "jdbc:mysql://180.76.153.111:3306/sample?autoReconnect=true"
  
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
    
    val reportEntityDF = sqlContext.read.json("/root/data/report.json")
    reportEntityDF.createTempView("report")
    //telphone => telephone, type => type_name
    val reportBaseDF = sqlContext.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_guarantee ,ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone, ent_base.type as type_name, report_at, year from report")
    reportBaseDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_base", prop)
  	
  	
  	//ent.reports.branchs
    val reportbranchDF = sqlContext.sql("select report_id, branchs from report where branchs is not null")
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
    val flatreportbranchDF = sqlContext.createDataFrame(reportbranchRDD, reportbranchSchema)
    flatreportbranchDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_branch", prop)
    
    
    //ent.reports.changes
    val reportChangeDF = sqlContext.sql("select report_id, changes from report where changes is not null")
    val reportChangeRDD = reportChangeDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("changes")
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
    val reportChangeStructType = reportChangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val reportChangeSchema = StructType(StructField("id", LongType, false) +: StructField("report_id", LongType, false) +: reportChangeStructType.fields)
    val flatreportChangeDF = sqlContext.createDataFrame(reportChangeRDD, reportChangeSchema)
    flatreportChangeDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_change", prop)
    
    
    //ent.reports.guarantees
    val reportGuaranteeDF = sqlContext.sql("select report_id, guarantees from report where guarantees is not null")
    val reportGuaranteeRDD = reportGuaranteeDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("guarantees")
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
    val reportGuaranteeStructType = reportGuaranteeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val reportGuaranteeSchema = StructType(StructField("id", LongType, false) +: StructField("report_id", LongType, false) +: reportGuaranteeStructType.fields)
    val flatreportGuaranteeDF = sqlContext.createDataFrame(reportGuaranteeRDD, reportGuaranteeSchema)
    flatreportGuaranteeDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_guarantee", prop)
    
    
    //ent.reports.inv_ents
    val invEntsDF = sqlContext.sql("select report_id, inv_ents from report where inv_ents is not null")
    val invEntsRDD = invEntsDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("inv_ents")
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
    val invEntsStructType = invEntsDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val invEntsSchema = StructType(StructField("id", LongType, false) +: StructField("report_id", LongType, false) +: invEntsStructType.fields)
    val flatinvEntsDF = sqlContext.createDataFrame(invEntsRDD, invEntsSchema)
    flatinvEntsDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_invest_enterprise", prop)
    
    
    //ent.reports.investment
    val reportInvestmentDF = sqlContext.sql("select report_id, investment from report where investment is not null")
    val reportInvestmentRDD = reportInvestmentDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("investment")
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
    val reportInvestmentStructType = reportInvestmentDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val reportInvestmentSchema = StructType(StructField("id", LongType, false) +: StructField("report_id", LongType, false) +: reportInvestmentStructType.fields)
    val flatreportinvestmentDF = sqlContext.createDataFrame(reportInvestmentRDD, reportInvestmentSchema)
    flatreportinvestmentDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_stock_investment", prop)
    
    
    //ent.reports.licenses
    val licenceDF = sqlContext.sql("select report_id, licenses from report where licenses is not null")
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
    val flatlicenceDF = sqlContext.createDataFrame(licenceRDD, licenceSchema)
    flatlicenceDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_licence", prop)
    
    
    //ent.reports.operation
    val operationDF = sqlContext.sql("select report_id, operation from report where operation is not null")
    val operationRDD = operationDF.rdd.map{
      x => {
        val row = x.getAs[Row]("operation")
        val reportId: Long = x.getAs[Long]("report_id")
        val cols = row.toSeq
        val newRow = Row.fromSeq(reportId+:cols)
        newRow
      }
    }.zipWithUniqueId().map {
      case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
    }
    val operationStructType = operationDF.schema.fields(1).dataType.asInstanceOf[StructType]
    val operationSchema = StructType(StructField("id", LongType, false) +: StructField("report_id", LongType, false) +: operationStructType.fields)
    val flatoperationDF = sqlContext.createDataFrame(operationRDD, operationSchema)
    flatoperationDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_operation", prop)
    
    
    //ent.reports.stock_changes
    val stockChangeDF = sqlContext.sql("select report_id, stock_changes from report where stock_changes is not null")
    val stockChangeRDD = stockChangeDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("stock_changes")
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
    val stockChangeStructType = stockChangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val stockChangeSchema = StructType(StructField("id", LongType, false) +: StructField("report_id", LongType, false) +: stockChangeStructType.fields)
    val flatstockChangeDF = sqlContext.createDataFrame(stockChangeRDD, stockChangeSchema)
    flatstockChangeDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_stock_change", prop)
    
    
    //ent.reports.websites
    val websiteDF = sqlContext.sql("select report_id, websites from report where websites is not null")
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
    val flatwebsiteDF = sqlContext.createDataFrame(websiteRDD, websiteSchema).withColumnRenamed("type", "type_name")
    flatwebsiteDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_report_website", prop)
  }
}