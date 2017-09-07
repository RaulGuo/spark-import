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

/**
 * 导入sample的数据，数据的schema为：
 * 
 root
 |-- bus: struct (nullable = true)
 |    |-- abnormals: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- add_at: string (nullable = true)
 |    |    |    |-- add_cause: string (nullable = true)
 |    |    |    |-- dec_org: string (nullable = true)
 |    |    |    |-- remove_at: string (nullable = true)
 |    |    |    |-- remove_cause: string (nullable = true)
 |    |-- base: struct (nullable = true)
 |    |    |-- address: string (nullable = true)
 |    |    |-- check_at: string (nullable = true)
 |    |    |-- credit_no: string (nullable = true)
 |    |    |-- end_at: string (nullable = true)
 |    |    |-- formation: string (nullable = true)
 |    |    |-- leg_rep: string (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- reg_capi: string (nullable = true)
 |    |    |-- reg_no: string (nullable = true)
 |    |    |-- reg_org: string (nullable = true)
 |    |    |-- revoked_at: string (nullable = true)
 |    |    |-- scope: string (nullable = true)
 |    |    |-- start_at: string (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- term_end_at: string (nullable = true)
 |    |    |-- term_start_at: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |-- branchs: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- reg_no: string (nullable = true)
 |    |    |    |-- reg_org: string (nullable = true)
 |    |-- changes: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- after: string (nullable = true)
 |    |    |    |-- before: string (nullable = true)
 |    |    |    |-- change_at: string (nullable = true)
 |    |    |    |-- item: string (nullable = true)
 |    |-- equity_pledges: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- equity_no: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- pledge_at: string (nullable = true)
 |    |    |    |-- pledgee: string (nullable = true)
 |    |    |    |-- pledgee_no: string (nullable = true)
 |    |    |    |-- pledgor: string (nullable = true)
 |    |    |    |-- pledgor_no: string (nullable = true)
 |    |    |    |-- pledgor_strand: string (nullable = true)
 |    |    |    |-- state: string (nullable = true)
 |    |-- investment: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- act_amount: string (nullable = true)
 |    |    |    |-- act_at: string (nullable = true)
 |    |    |    |-- act_type: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- sub_amount: string (nullable = true)
 |    |    |    |-- sub_at: string (nullable = true)
 |    |    |    |-- sub_type: string (nullable = true)
 |    |    |    |-- total_act_amount: string (nullable = true)
 |    |    |    |-- total_sub_amount: string (nullable = true)
 |    |-- members: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- position: string (nullable = true)
 |    |-- mortgages: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- collateral: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |-- ownership: string (nullable = true)
 |    |    |    |    |    |-- remark: string (nullable = true)
 |    |    |    |    |    |-- status: string (nullable = true)
 |    |    |    |-- debt_secured: struct (nullable = true)
 |    |    |    |    |-- amount: string (nullable = true)
 |    |    |    |    |-- debt_term: string (nullable = true)
 |    |    |    |    |-- kind: string (nullable = true)
 |    |    |    |    |-- remark: string (nullable = true)
 |    |    |    |    |-- scope: string (nullable = true)
 |    |    |    |-- mortgage_reg: struct (nullable = true)
 |    |    |    |    |-- debt_amount: string (nullable = true)
 |    |    |    |    |-- debt_term: string (nullable = true)
 |    |    |    |    |-- debut_type: string (nullable = true)
 |    |    |    |    |-- no: string (nullable = true)
 |    |    |    |    |-- reg_at: string (nullable = true)
 |    |    |    |    |-- reg_org: string (nullable = true)
 |    |    |    |    |-- remark: string (nullable = true)
 |    |    |    |    |-- secure_scope: string (nullable = true)
 |    |    |    |    |-- state: string (nullable = true)
 |    |    |    |-- mortgagee: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- cert_no: string (nullable = true)
 |    |    |    |    |    |-- cert_type: string (nullable = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |-- punishs: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- content: string (nullable = true)
 |    |    |    |-- dec_at: string (nullable = true)
 |    |    |    |-- detail: string (nullable = true)
 |    |    |    |-- leg_rep: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- org_name: string (nullable = true)
 |    |    |    |-- paper_no: string (nullable = true)
 |    |    |    |-- reg_no: string (nullable = true)
 |    |    |    |-- remark: string (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |    |-- spot_checks: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- check_at: string (nullable = true)
 |    |    |    |-- check_org: string (nullable = true)
 |    |    |    |-- result: string (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |    |-- stockholders: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- cert_no: string (nullable = true)
 |    |    |    |-- cert_type: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |-- ent: struct (nullable = true)
 |    |-- changes: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- after: string (nullable = true)
 |    |    |    |-- before: string (nullable = true)
 |    |    |    |-- change_at: string (nullable = true)
 |    |    |    |-- item: string (nullable = true)
 |    |-- intellectuals: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- kind: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- no: string (nullable = true)
 |    |    |    |-- pledgee: string (nullable = true)
 |    |    |    |-- pledgor: string (nullable = true)
 |    |    |    |-- state: string (nullable = true)
 |    |    |    |-- term: string (nullable = true)
 |    |-- investment: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- act_amount: string (nullable = true)
 |    |    |    |-- act_at: string (nullable = true)
 |    |    |    |-- act_type: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- sub_amount: string (nullable = true)
 |    |    |    |-- sub_at: string (nullable = true)
 |    |    |    |-- sub_type: string (nullable = true)
 |    |    |    |-- total_act_amount: string (nullable = true)
 |    |    |    |-- total_sub_amount: string (nullable = true)
 |    |-- licenses: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- content: string (nullable = true)
 |    |    |    |-- end_at: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- no: string (nullable = true)
 |    |    |    |-- org: string (nullable = true)
 |    |    |    |-- start_at: string (nullable = true)
 |    |    |    |-- state: string (nullable = true)
 |    |-- punishs: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- content: string (nullable = true)
 |    |    |    |-- dec_at: string (nullable = true)
 |    |    |    |-- detail: string (nullable = true)
 |    |    |    |-- leg_rep: string (nullable = true)
 |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- org_name: string (nullable = true)
 |    |    |    |-- paper_no: string (nullable = true)
 |    |    |    |-- reg_no: string (nullable = true)
 |    |    |    |-- remark: string (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |    |-- reports: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- branchs: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |-- reg_no: string (nullable = true)
 |    |    |    |    |    |-- reg_org: string (nullable = true)
 |    |    |    |-- changes: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- after: string (nullable = true)
 |    |    |    |    |    |-- before: string (nullable = true)
 |    |    |    |    |    |-- change_at: string (nullable = true)
 |    |    |    |    |    |-- item: string (nullable = true)
 |    |    |    |-- ent_base: struct (nullable = true)
 |    |    |    |    |-- address: string (nullable = true)
 |    |    |    |    |-- amount: string (nullable = true)
 |    |    |    |    |-- credit_no: string (nullable = true)
 |    |    |    |    |-- email: string (nullable = true)
 |    |    |    |    |-- employ_num: string (nullable = true)
 |    |    |    |    |-- is_guarantee: string (nullable = true)
 |    |    |    |    |-- is_invest: string (nullable = true)
 |    |    |    |    |-- is_stock: string (nullable = true)
 |    |    |    |    |-- is_website: string (nullable = true)
 |    |    |    |    |-- leg_rep: string (nullable = true)
 |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |-- postcode: string (nullable = true)
 |    |    |    |    |-- reg_no: string (nullable = true)
 |    |    |    |    |-- relationship: string (nullable = true)
 |    |    |    |    |-- state: string (nullable = true)
 |    |    |    |    |-- telphone: string (nullable = true)
 |    |    |    |    |-- type: string (nullable = true)
 |    |    |    |-- guarantees: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- creditor: string (nullable = true)
 |    |    |    |    |    |-- debt_amount: string (nullable = true)
 |    |    |    |    |    |-- debt_kind: string (nullable = true)
 |    |    |    |    |    |-- debt_term: string (nullable = true)
 |    |    |    |    |    |-- debtor: string (nullable = true)
 |    |    |    |    |    |-- guar_range: string (nullable = true)
 |    |    |    |    |    |-- guar_term: string (nullable = true)
 |    |    |    |    |    |-- guar_type: string (nullable = true)
 |    |    |    |-- inv_ents: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |-- reg_no: string (nullable = true)
 |    |    |    |-- investment: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- act_amount: string (nullable = true)
 |    |    |    |    |    |-- act_at: string (nullable = true)
 |    |    |    |    |    |-- act_type: string (nullable = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |-- sub_amount: string (nullable = true)
 |    |    |    |    |    |-- sub_at: string (nullable = true)
 |    |    |    |    |    |-- sub_type: string (nullable = true)
 |    |    |    |-- licenses: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- end_at: string (nullable = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- operation: struct (nullable = true)
 |    |    |    |    |-- financial_loan: string (nullable = true)
 |    |    |    |    |-- fund_subsidy: string (nullable = true)
 |    |    |    |    |-- main_income: string (nullable = true)
 |    |    |    |    |-- net_profit: string (nullable = true)
 |    |    |    |    |-- profit: string (nullable = true)
 |    |    |    |    |-- total_asset: string (nullable = true)
 |    |    |    |    |-- total_debt: string (nullable = true)
 |    |    |    |    |-- total_equity: string (nullable = true)
 |    |    |    |    |-- total_tax: string (nullable = true)
 |    |    |    |    |-- total_turnover: string (nullable = true)
 |    |    |    |-- report_at: string (nullable = true)
 |    |    |    |-- stock_changes: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- after: string (nullable = true)
 |    |    |    |    |    |-- before: string (nullable = true)
 |    |    |    |    |    |-- change_at: string (nullable = true)
 |    |    |    |    |    |-- stockholder: string (nullable = true)
 |    |    |    |-- stockholders: string (nullable = true)
 |    |    |    |-- websites: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |-- type: string (nullable = true)
 |    |    |    |    |    |-- url: string (nullable = true)
 |    |    |    |-- year: string (nullable = true)
 |    |-- stock_changes: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- after: string (nullable = true)
 |    |    |    |-- before: string (nullable = true)
 |    |    |    |-- change_at: string (nullable = true)
 |    |    |    |-- stockholder: string (nullable = true)
 |-- company_id: long (nullable = false)
 */


object ImportSample {
  val conf = new SparkConf().setAppName("DataImport").setMaster("local").set("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse")
  val sc = new SparkContext(conf)
  val dbUrl = "jdbc:mysql://180.76.153.111:3306/sample?autoReconnect=true"
  
  def main(args: Array[String]): Unit = {
    
    
    if(args == null){
      System.exit(0)
    }
    
    val start = System.currentTimeMillis()
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
    
    
    
    val compBaseDF = sqlContext.sql("select company_id as id, bus.base.address,bus.base.check_at,bus.base.credit_no,bus.base.end_at,bus.base.formation,bus.base.leg_rep,bus.base.name,bus.base.reg_capi,bus.base.reg_no,bus.base.reg_org,bus.base.scope,bus.base.start_at,bus.base.state,bus.base.term_end_at,bus.base.term_start_at,bus.base.type from company")
    compBaseDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company", prop)
    
    //bus.abnormals的处理
    val abnormalDF = sqlContext.sql("select company_id, bus.abnormals from company where bus.abnormals is not null")
    val abnormalRDD = abnormalDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("abnormals")
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
    val abnormalStructType = abnormalDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val abnormalSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: abnormalStructType.fields)
    val flatAbnormalDF = sqlContext.createDataFrame(abnormalRDD, abnormalSchema)
    flatAbnormalDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_abnormal", prop)
    
    
    //bus.branchs
    val branchDF = sqlContext.sql("select company_id, bus.branchs from company where bus.branchs is not null")
    val branchRDD = branchDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("branchs")
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
    val branchStructType = branchDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val branchSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: branchStructType.fields)
    val flatbranchDF = sqlContext.createDataFrame(branchRDD, branchSchema)
    flatbranchDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_branch", prop)
    
    
    //bus.changes
    val changeDF = sqlContext.sql("select company_id, bus.changes as change from company where bus.changes is not null")
  	val changeRDD = changeDF.rdd.flatMap{
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
    val changeStructType = changeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val changeSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: changeStructType.fields)
    val flatChangeDF = sqlContext.createDataFrame(changeRDD, changeSchema)
    flatChangeDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_change", prop)
    
    
    //bus.equity_pledges
    val equityPledgeDF = sqlContext.sql("select company_id, bus.equity_pledges from company where bus.equity_pledges is not null")
    val equityPledgeRDD = equityPledgeDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("equity_pledges")
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
    val equityPledgeStructType = equityPledgeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val equityPledgeSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: equityPledgeStructType.fields)
    val flatequityPledgeDF = sqlContext.createDataFrame(equityPledgeRDD, equityPledgeSchema)
    flatequityPledgeDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_equity_pledge", prop)
    
    
    //bus.investment
    //企业的股东及出资信息
    val compinvestmentDF = sqlContext.sql("select company_id, bus.investment from company where bus.investment is not null")
    val compinvestmentRDD = compinvestmentDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("investment")
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
    val compinvestmentStructType = compinvestmentDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val compinvestmentSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: compinvestmentStructType.fields)
    val flatcompinvestmentDF = sqlContext.createDataFrame(compinvestmentRDD, compinvestmentSchema)
    flatcompinvestmentDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_stock_investment", prop)
    
    
    //bus.members
    val memberDF = sqlContext.sql("select company_id, bus.members from company where bus.members is not null")
    val memberRDD = memberDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("members")
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
    val memberStructType = memberDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val memberSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: memberStructType.fields)
    val flatmemberDF = sqlContext.createDataFrame(memberRDD, memberSchema)
    flatmemberDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_family_member", prop)
    
    //bus.mortgages
    
    //bus.mortgages.debt_secured
    val mortgageDebtSecureDF = sqlContext.sql("select company_id, bus.mortgages.debt_secured from company where bus.mortgages.debt_secured is not null")
    val mortgageDebtSecureRDD = mortgageDebtSecureDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("debt_secured")
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
    val mortgageDebtSecureStructType = mortgageDebtSecureDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val mortgageDebtSecureSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: mortgageDebtSecureStructType.fields)
    val flatmortgageDebtSecureDF = sqlContext.createDataFrame(mortgageDebtSecureRDD, mortgageDebtSecureSchema).withColumnRenamed("type", "type_name")
    flatmortgageDebtSecureDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_mortgage_debt_secure", prop)
    
    
    //bus.mortgages.mortgage_reg
    val mortgageRegisterDF = sqlContext.sql("select company_id, bus.mortgages.mortgage_reg from company where bus.mortgages.mortgage_reg is not null")
    val mortgageRegisterRDD = mortgageRegisterDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("mortgage_reg")
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
    val mortgageRegisterStructType = mortgageRegisterDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val mortgageRegisterSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: mortgageRegisterStructType.fields)
    val flatmortgageRegisterDF = sqlContext.createDataFrame(mortgageRegisterRDD, mortgageRegisterSchema).withColumnRenamed("type", "type_name")
    flatmortgageRegisterDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_mortgage_debt_secure", prop)
    
    
    //bus.mortgages.collateral
    val mortgagecollateralDF = sqlContext.sql("select company_id, bus.mortgages.collateral from company where bus.mortgages.collateral is not null")
    val mortgagecollateralRDD = mortgagecollateralDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[WrappedArray[Row]]]("collateral")
        val compId: Long = x.getAs[Long]("company_id")
        var list = List[Row]()
        rows.foreach { rowArray => {
          if(rowArray != null){//一对多对多，所以需要两层迭代
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
    val flatmortgagecollateralDF = sqlContext.createDataFrame(mortgagecollateralRDD, mortgagecollateralSchema)
    flatmortgagecollateralDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_mortgage_debt_secure", prop)
    
    
    //bus.mortgages.mortgagee
    val mortgageDF = sqlContext.sql("select company_id, bus.mortgages.mortgagee from company where bus.mortgages.mortgagee is not null")
    val mortgageRDD = mortgageDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[WrappedArray[Row]]]("")
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
    val mortgageStructType = mortgageDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val mortgageSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: mortgageStructType.fields)
    val flatmortgageDF = sqlContext.createDataFrame(mortgageRDD, mortgageSchema)
    flatmortgageDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_mortgagee", prop)
    
    
    
    
    //bus.punishs
    val punishDF = sqlContext.sql("select company_id, bus.punishs from company where bus.punishs is not null")
    val punishRDD = punishDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("punishs")
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
    val punishStructType = punishDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val punishSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: punishStructType.fields)
    val flatpunishDF = sqlContext.createDataFrame(punishRDD, punishSchema).withColumnRenamed("type", "type_name")
    flatpunishDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_punish_icbc", prop)
    
    
    //bus.spot_checks
    val spotCheckDF = sqlContext.sql("select company_id, bus.spot_checks from company where bus.spot_checks is not null")
    val spotCheckRDD = spotCheckDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("spot_checks")
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
    val spotCheckStructType = spotCheckDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val spotCheckSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: spotCheckStructType.fields)
    val flatspotCheckDF = sqlContext.createDataFrame(spotCheckRDD, spotCheckSchema).withColumnRenamed("type", "type_name")
    flatspotCheckDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_spot_check", prop)
    
    
    //bus.stockholders
    val stockholderDF = sqlContext.sql("select company_id, bus.stockholders from company where bus.stockholders is not null")
    val stockholderRDD = stockholderDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("stockholders")
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
    val stockholderStructType = stockholderDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val stockholderSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: stockholderStructType.fields)
    val flatstockholderDF = sqlContext.createDataFrame(stockholderRDD, stockholderSchema).withColumnRenamed("type", "type_name")
    flatstockholderDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_stock_holder", prop)
    
    
    //ent.changes
    val entchangeDF = sqlContext.sql("select company_id, ent.changes as change from company where ent.changes is not null")
  	val entchangeRDD = entchangeDF.rdd.flatMap{
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
    val entchangeStructType = entchangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val entchangeSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: entchangeStructType.fields)
    val flatentChangeDF = sqlContext.createDataFrame(entchangeRDD, changeSchema)
    flatentChangeDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_ent_change", prop)
    
    
    
    //ent.intellectuals
    val intellectualDF = sqlContext.sql("select company_id, ent.intellectuals from company where ent.intellectuals is not null")
    val intellectualRDD = intellectualDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("intellectuals")
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
    val intellectualStructType = intellectualDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val intellectualSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: intellectualStructType.fields)
    val flatintellectualDF = sqlContext.createDataFrame(intellectualRDD, intellectualSchema)
    flatintellectualDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_intellectual", prop)
    
    
    
    //ent.investment
    val investmentDF = sqlContext.sql("select company_id, ent.investment from company where ent.investment is not null")
    val investmentRDD = investmentDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("investment")
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
    val investmentStructType = investmentDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val investmentSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: investmentStructType.fields)
    val flatinvestmentDF = sqlContext.createDataFrame(investmentRDD, investmentSchema)
    flatinvestmentDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_investment", prop)
    
    
    
    //ent.licenses
    val licenseDF = sqlContext.sql("select company_id, ent.licenses from company where ent.licenses is not null")
    val licenseRDD = licenseDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("licenses")
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
    val licenseStructType = licenseDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val licenseSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: licenseStructType.fields)
    val flatlicenseDF = sqlContext.createDataFrame(licenseRDD, licenseSchema)
    flatlicenseDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_license", prop)
    
    
    //ent.punishs
    val comppunishDF = sqlContext.sql("select company_id, ent.punishs from company where ent.punishs is not null")
    val comppunishRDD = comppunishDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("punishs")
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
    val comppunishStructType = comppunishDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val comppunishSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: comppunishStructType.fields)
    val flatcomppunishDF = sqlContext.createDataFrame(comppunishRDD, comppunishSchema).withColumnRenamed("type", "type_name")
    flatcomppunishDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_punish_ent", prop)
    
    
    
    //ent.stock_changes
    val compstockChangeDF = sqlContext.sql("select company_id, ent.stock_changes from company where ent.stock_changes is not null")
    val compstockChangeRDD = compstockChangeDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[Row]]("stock_changes")
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
    val compstockChangeStructType = compstockChangeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
    val compstockChangeSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: compstockChangeStructType.fields)
    val flatcompstockChangeDF = sqlContext.createDataFrame(compstockChangeRDD, compstockChangeSchema)
    flatcompstockChangeDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_stock_change", prop)
    
    
    //-------------------------------------------------
    //报表信息的导入：
    //报表的schema：
    /**
      |    |-- reports: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- branchs: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |-- reg_no: string (nullable = true)
 |    |    |    |    |    |-- reg_org: string (nullable = true)
 |    |    |    |-- changes: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- after: string (nullable = true)
 |    |    |    |    |    |-- before: string (nullable = true)
 |    |    |    |    |    |-- change_at: string (nullable = true)
 |    |    |    |    |    |-- item: string (nullable = true)
 |    |    |    |-- ent_base: struct (nullable = true)
 |    |    |    |    |-- address: string (nullable = true)
 |    |    |    |    |-- amount: string (nullable = true)
 |    |    |    |    |-- credit_no: string (nullable = true)
 |    |    |    |    |-- email: string (nullable = true)
 |    |    |    |    |-- employ_num: string (nullable = true)
 |    |    |    |    |-- is_guarantee: string (nullable = true)
 |    |    |    |    |-- is_invest: string (nullable = true)
 |    |    |    |    |-- is_stock: string (nullable = true)
 |    |    |    |    |-- is_website: string (nullable = true)
 |    |    |    |    |-- leg_rep: string (nullable = true)
 |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |-- postcode: string (nullable = true)
 |    |    |    |    |-- reg_no: string (nullable = true)
 |    |    |    |    |-- relationship: string (nullable = true)
 |    |    |    |    |-- state: string (nullable = true)
 |    |    |    |    |-- telphone: string (nullable = true)
 |    |    |    |    |-- type: string (nullable = true)
 |    |    |    |-- guarantees: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- creditor: string (nullable = true)
 |    |    |    |    |    |-- debt_amount: string (nullable = true)
 |    |    |    |    |    |-- debt_kind: string (nullable = true)
 |    |    |    |    |    |-- debt_term: string (nullable = true)
 |    |    |    |    |    |-- debtor: string (nullable = true)
 |    |    |    |    |    |-- guar_range: string (nullable = true)
 |    |    |    |    |    |-- guar_term: string (nullable = true)
 |    |    |    |    |    |-- guar_type: string (nullable = true)
 |    |    |    |-- inv_ents: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |-- reg_no: string (nullable = true)
 |    |    |    |-- investment: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- act_amount: string (nullable = true)
 |    |    |    |    |    |-- act_at: string (nullable = true)
 |    |    |    |    |    |-- act_type: string (nullable = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |-- sub_amount: string (nullable = true)
 |    |    |    |    |    |-- sub_at: string (nullable = true)
 |    |    |    |    |    |-- sub_type: string (nullable = true)
 |    |    |    |-- licenses: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- end_at: string (nullable = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- operation: struct (nullable = true)
 |    |    |    |    |-- financial_loan: string (nullable = true)
 |    |    |    |    |-- fund_subsidy: string (nullable = true)
 |    |    |    |    |-- main_income: string (nullable = true)
 |    |    |    |    |-- net_profit: string (nullable = true)
 |    |    |    |    |-- profit: string (nullable = true)
 |    |    |    |    |-- total_asset: string (nullable = true)
 |    |    |    |    |-- total_debt: string (nullable = true)
 |    |    |    |    |-- total_equity: string (nullable = true)
 |    |    |    |    |-- total_tax: string (nullable = true)
 |    |    |    |    |-- total_turnover: string (nullable = true)
 |    |    |    |-- report_at: string (nullable = true)
 |    |    |    |-- stock_changes: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- after: string (nullable = true)
 |    |    |    |    |    |-- before: string (nullable = true)
 |    |    |    |    |    |-- change_at: string (nullable = true)
 |    |    |    |    |    |-- stockholder: string (nullable = true)
 |    |    |    |-- stockholders: string (nullable = true)
 |    |    |    |-- websites: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |    |    |-- type: string (nullable = true)
 |    |    |    |    |    |-- url: string (nullable = true)
 |    |    |    |-- year: string (nullable = true)
     */
    
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
  	
  	//report base
  	reportEntityDF.createTempView("report")
    val reportBaseDF = sqlContext.sql("select report_id as id, company_id, ent_base.address, ent_base.amount, ent_base.credit_no, ent_base.email, ent_base.employ_num, ent_base.is_invest, ent_base.is_stock, ent_base.is_website, ent_base.leg_rep, ent_base.name, ent_base.postcode, ent_base.reg_no, ent_base.relationship, ent_base.state, ent_base.telphone as telephone, ent_base.type as type_name, report_at, stockholders, year from report")
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
    
    
    val end = System.currentTimeMillis()
    
    println("import sample cost time:"+(end-start)+"ms")
    
    
  }
}