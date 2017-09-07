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

object ImportCompanyByJson {
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
    
    val df = sqlContext.read.json("/root/data/company.json")
    
    df.createTempView("company")
    
    
    
//    val compBaseDF = sqlContext.sql("select company_id as id, bus.base.address,bus.base.check_at,bus.base.credit_no,bus.base.end_at,bus.base.formation,bus.base.leg_rep,bus.base.name,bus.base.reg_capi,bus.base.reg_no,bus.base.reg_org,bus.base.scope,bus.base.start_at,bus.base.state,bus.base.term_end_at,bus.base.term_start_at,bus.base.type from company")
//    compBaseDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company", prop)
//    
//    //bus.abnormals的处理
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
//    
//    
//    //bus.branchs
//    val branchDF = sqlContext.sql("select company_id, bus.branchs from company where bus.branchs is not null")
//    val branchRDD = branchDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("branchs")
//        val compId: Long = x.getAs[Long]("company_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(compId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }.zipWithUniqueId().map {
//      case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
//    }
//    val branchStructType = branchDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val branchSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: branchStructType.fields)
//    val flatbranchDF = sqlContext.createDataFrame(branchRDD, branchSchema)
//    flatbranchDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_branch", prop)
//    
//    
//    //bus.changes
//    val changeDF = sqlContext.sql("select company_id, bus.changes as change from company where bus.changes is not null")
//  	val changeRDD = changeDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("change")
//        val compId: Long = x.getAs[Long]("company_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(compId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }.zipWithUniqueId().map {
//      case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
//    }
//    val changeStructType = changeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val changeSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: changeStructType.fields)
//    val flatChangeDF = sqlContext.createDataFrame(changeRDD, changeSchema)
//    flatChangeDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_change", prop)
//    
//    
//    //bus.equity_pledges
//    val equityPledgeDF = sqlContext.sql("select company_id, bus.equity_pledges from company where bus.equity_pledges is not null")
//    val equityPledgeRDD = equityPledgeDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("equity_pledges")
//        val compId: Long = x.getAs[Long]("company_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(compId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }.zipWithUniqueId().map {
//      case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
//    }
//    val equityPledgeStructType = equityPledgeDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val equityPledgeSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: equityPledgeStructType.fields)
//    val flatequityPledgeDF = sqlContext.createDataFrame(equityPledgeRDD, equityPledgeSchema)
//    flatequityPledgeDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_equity_pledge", prop)
//    
//    
//    //bus.investment
//    //企业的股东及出资信息
//    val compinvestmentDF = sqlContext.sql("select company_id, bus.investment from company where bus.investment is not null")
//    val compinvestmentRDD = compinvestmentDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("investment")
//        val compId: Long = x.getAs[Long]("company_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(compId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }.zipWithUniqueId().map {
//      case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
//    }
//    val compinvestmentStructType = compinvestmentDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val compinvestmentSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: compinvestmentStructType.fields)
//    val flatcompinvestmentDF = sqlContext.createDataFrame(compinvestmentRDD, compinvestmentSchema)
//    flatcompinvestmentDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_stock_investment", prop)
//    
//    
//    //bus.members
//    val memberDF = sqlContext.sql("select company_id, bus.members from company where bus.members is not null")
//    val memberRDD = memberDF.rdd.flatMap{
//      x => {
//        val rows = x.getAs[WrappedArray[Row]]("members")
//        val compId: Long = x.getAs[Long]("company_id")
//        var list = List[Row]()
//        rows.foreach { row => {
//          val cols = row.toSeq
//          val newRow = Row.fromSeq(compId+:cols)
//          list = list:+newRow
//        } }
//        list.toArray[Row]
//      }
//    }.zipWithUniqueId().map {
//      case (r: Row, id: Long) => Row.fromSeq(id+:r.toSeq)
//    }
//    val memberStructType = memberDF.schema.fields(1).dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
//    val memberSchema = StructType(StructField("id", LongType, false) +: StructField("company_id", LongType, false) +: memberStructType.fields)
//    val flatmemberDF = sqlContext.createDataFrame(memberRDD, memberSchema)
//    flatmemberDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_family_member", prop)
    
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
    flatmortgageRegisterDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_mortgage_register", prop)
    
    
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
    flatmortgagecollateralDF.write.mode(SaveMode.Append).jdbc(dbUrl, "company_mortgage_collateral", prop)
    
    
    //bus.mortgages.mortgagee
    val mortgageDF = sqlContext.sql("select company_id, bus.mortgages.mortgagee from company where bus.mortgages.mortgagee is not null")
    val mortgageRDD = mortgageDF.rdd.flatMap{
      x => {
        val rows = x.getAs[WrappedArray[WrappedArray[Row]]]("mortgagee")
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
    val flatentChangeDF = sqlContext.createDataFrame(entchangeRDD, entchangeSchema)
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
    
    
    
  }
}