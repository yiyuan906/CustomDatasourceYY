package com.yiyuan.v2.customds

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, PrunedScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

class customdsRelation(relationContext:SQLContext, relationPath:String, readMode:String, endpoint:String, accesskey:String,
                       secretkey:String) extends BaseRelation with PrunedScan{
  override def sqlContext: SQLContext = relationContext

  override def schema: StructType = new StructType()
    .add(StructField("Date",DateType,true))
    .add(StructField("Time",StringType,true))
    .add(StructField("Action",StringType,true))
    .add(StructField("Role",StringType,true))
    .add(StructField("ActionID",IntegerType,true))
    .add(StructField("Description",StringType,true))

  override def buildScan(requiredColumns: Array[String]): RDD[Row] =
    new customdsRelationComp(relationContext,requiredColumns,relationPath,readMode,endpoint,accesskey,secretkey)
}
