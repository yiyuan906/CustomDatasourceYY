package com.yiyuan.v3.dataloader

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.spark.sql.Row

import scala.util.{Failure, Success, Try}

class datasourceCRelation extends getAndConvert {
  override def getStream(endpt: String, accesskey: String, secretkey: String, path: String): AnyRef = {
    val hConf = new Configuration()
    hConf.set("fs.s3a.endpoint", endpt)
    hConf.set("fs.s3a.access.key", accesskey)
    hConf.set("fs.s3a.secret.key", secretkey)
    val nPath = new Path(s"s3a://$path")
    val parReader = ParquetReader.builder(new GroupReadSupport(),nPath).withConf(hConf).build()
    parReader
  }

  override def getRow(currentObject: Any): Row = {
    val parObject = currentObject.asInstanceOf[ParquetReader[Group]]
    val parquetLine = parObject.read()
    Try(Row(parquetLine.getValueToString(0,0),parquetLine.getValueToString(1,0)
      ,parquetLine.getValueToString(2,0),parquetLine.getValueToString(3,0)
      ,parquetLine.getValueToString(4,0).toInt,parquetLine.getValueToString(5,0)))
    match {
      case Success(row) => row
      case Failure(x) => Row.empty
    }
  }

  //For this type of iterator which only knows when it ends when it ends, the process of going into the next
  //stream is done at the next() of the main iterator when it sees that the row returned is Row.empty.
  //So for this hasNext check its always true. (applicable for other iterator which are like this as well)
  override def hasNextT(currentObject: Any): Boolean = {
    true
  }
}
