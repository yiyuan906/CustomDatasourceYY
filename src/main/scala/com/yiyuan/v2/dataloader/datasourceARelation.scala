package com.yiyuan.v2.dataloader

import org.apache.spark.sql.Row

class datasourceARelation extends readType {
  def streamParsing(stringStream:String): Row = {
    val split = stringStream.split(" ")
    val desc = stringStream.drop((split(0) + split(1) + split(2) + split(3) + split(4) + split(5)).length + 6)
    Row(split(0),split(1), split(3), split(2), split(5).toInt, desc)
  }

  override def wayOfReading(): String = "text"
}
