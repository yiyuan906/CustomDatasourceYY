package com.yiyuan.v2.dataloader

import org.apache.spark.sql.Row

class datasourceBRelation extends readType {
  def streamParsing(stringStream:String): Row = {
    val split = stringStream.split(",")
    val desc = stringStream.drop((split(0)+split(1)+split(2)+split(3)+split(4)+split(5)+split(6)+split(7)).length+8)
    Row(split(1),split(4),split(7),split(2),split(5).toInt,desc)
  }

  override def wayOfReading(): String = "text"
}
