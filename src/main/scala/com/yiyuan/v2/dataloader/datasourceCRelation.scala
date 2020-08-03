package com.yiyuan.v2.dataloader

import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.spark.sql.Row

class datasourceCRelation extends readType {
  def singleStream(parquetStreamIn:ParquetReader[Group]): Row = {
    val parquetStream = parquetStreamIn.read()
    Row(parquetStream.getValueToString(0,0),parquetStream.getValueToString(1,0)
    ,parquetStream.getValueToString(2,0),parquetStream.getValueToString(3,0)
    ,parquetStream.getValueToString(4,0).toInt,parquetStream.getValueToString(5,0))
  }

  override def wayOfReading(): String = "parquet"
}
