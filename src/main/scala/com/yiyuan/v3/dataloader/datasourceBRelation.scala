package com.yiyuan.v3.dataloader

import io.minio.MinioClient
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.Row

class datasourceBRelation extends getAndConvert {
  override def getStream(endpt: String, accesskey: String, secretkey: String, path: String): AnyRef = {
    val minio = new MinioClient(endpt,accesskey,secretkey)
    val minioStream = minio.getObject(path.split("/")(0),path.drop(path.split("/")(0).size+1))
    val returnedIterator = IOUtils.readLines(minioStream,"UTF-8").iterator()
    returnedIterator.next()         //remove first row which is headers row
    returnedIterator
  }

  override def getRow(currentObject: Any): Row = {
    val stream = currentObject.asInstanceOf[java.util.Iterator[String]]
    val curString = stream.next()
    val split = curString.split(",")
    val desc = curString.drop((split(0)+split(1)+split(2)+split(3)+split(4)+split(5)+split(6)+split(7)).length+8)
    Row(split(1),split(4),split(7),split(2),split(5).toInt,desc) //Date.valueOf(split(1))
  }

  override def hasNextT(currentObject: Any): Boolean = {
    currentObject.asInstanceOf[java.util.Iterator[String]].hasNext
  }
}
