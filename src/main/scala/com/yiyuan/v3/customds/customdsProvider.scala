package com.yiyuan.v3.customds

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

import scala.util.{Failure, Success, Try}

class customdsProvider extends DataSourceRegister with RelationProvider {

  override def shortName(): String = "ySource3"

  override def createRelation(sqlContextCR: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new customdsRelation(sqlContextCR,
      parameters("path"),
      Try(parameters("readMode")) match {   //checks for .option("readMode","specific")
        case Success(output) => output
        case Failure(x) => ""
      },
      parameters("endpoint"),
      parameters("accesskey"),
      parameters("secretkey")
      )
  }
}
