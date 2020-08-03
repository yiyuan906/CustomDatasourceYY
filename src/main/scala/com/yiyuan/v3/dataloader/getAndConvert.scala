package com.yiyuan.v3.dataloader

import org.apache.spark.sql.Row

trait getAndConvert {
  /*Checks the iterator if it hasNext for the respective file*/
  def hasNextT(currentObject:Any):Boolean

  /*Uses the reader required to get the iterator for the file. AnyRef will be able to account for all the different Iterators
  from the different readers in this case.*/
  def getStream(endpt:String,accesskey:String,secretkey:String,path:String):AnyRef

  /*The iterator gotten previously will then be casted back here to be read per line and be converted into rows which are required.*/
  def getRow(currentObject:Any):Row
}
