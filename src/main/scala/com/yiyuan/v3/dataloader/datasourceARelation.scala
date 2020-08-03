package com.yiyuan.v3.dataloader

import io.minio.MinioClient
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.Row

class datasourceARelation extends getAndConvert {
  override def getStream(endpt: String, accesskey: String, secretkey: String, path:String): AnyRef = {
    val minio = new MinioClient(endpt,accesskey,secretkey)
    val minioStream = minio.getObject(path.split("/")(0),path.drop(path.split("/")(0).size+1))
    IOUtils.readLines(minioStream,"UTF-8").iterator()
  }

  override def getRow(currentObject: Any): Row = {
    val stream = currentObject.asInstanceOf[java.util.Iterator[String]]
    val curString = stream.next()
    val split = curString.split(" ")
    val desc = curString.drop((split(0) + split(1) + split(2) + split(3) + split(4) + split(5)).length + 6)
    Row(split(0),split(1), split(3), split(2), split(5).toInt, desc)
  }

  override def hasNextT(currentObject: Any): Boolean = {
    currentObject.asInstanceOf[java.util.Iterator[String]].hasNext
  }
}
