package com.yiyuan.v3.customds

import java.sql.Date

import com.yiyuan.v3.dataloader.getAndConvert
import io.minio.MinioClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class customdsRelationComp(compContext:SQLContext, columns: Array[String], compPath:String,
                           readMode:String, endpoint:String, accesskey:String, secretkey:String)
  extends RDD[Row](compContext.sparkContext, Nil) {

  val bucket = compPath.split("/")(0)

  def getMetadata(streamPath: String, minio:MinioClient):String = {
    if (!minio.bucketExists(bucket))
      throw new Exception("Bucket does not exist")

    Try(minio.statObject(bucket, streamPath).httpHeaders().get("X-Amz-Meta-User.datasourceclass")) match {
      case Success(objData) =>
        if(objData == null)
          "Empty"
        else
          objData.get(0)
      case Failure(x) => throw new Exception("Object does not exist")
    }
  }

  //Checks for objects in directory or all objects specified
  def getObjList(streamPath:String, minio:MinioClient):Array[String] = {
    if(readMode == "specific"){           //expects list to be given seperated by commas "bucket/dir/obj,bucket/dir/obj"
      streamPath.split(",").map(path=>path.drop(bucket.size+1))
    }
    else {                                //reads all under directory
      val objite = minio.listObjects(bucket,streamPath.drop(bucket.size+1)).iterator()
      val listBuffer = new ArrayBuffer[String]
      while(objite.hasNext)
        listBuffer += objite.next().get().objectName()

      listBuffer.foreach(println)
      listBuffer.toArray[String]
    }
  }

  def getStream(metadata:String, endpt:String, accesskey:String, secretkey:String,path:String):AnyRef = {
    val classInst = Class.forName(metadata).newInstance().asInstanceOf[getAndConvert]
    classInst.getStream(endpt,accesskey,secretkey,path)
  }

  def getRow(currentObject:AnyRef, metadata:String):Row = {
    val classInst = Class.forName(metadata).newInstance().asInstanceOf[getAndConvert]
    classInst.getRow(currentObject)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val minioObj = new MinioClient(endpoint, accesskey, secretkey)
    val listOfObj = getObjList(compPath, minioObj)

    //getting Array(path,metadata)
    val objAndMetadata = listOfObj.map{fullPath=>
      (fullPath,getMetadata(fullPath,minioObj))
    }

    val filteredObjAndMetadata = objAndMetadata.filter(value=>{ //filter objects with empty metadata
      if(value._2.contains("Empty"))        //"Empty" gotten when nothing read from metadata
        false
      else true
    })

    //getting Array(dataIterator,metadata)
    val fullDetailsOfObj = filteredObjAndMetadata.map{value=>
      val metadata = value._2
      val path = value._1
      (getStream(metadata,endpoint,accesskey,secretkey,bucket+"/"+path),metadata)
    }

    val (arrayOfObjs,arrayOfMetadata) = fullDetailsOfObj.unzip

    val iteOfObjs = arrayOfObjs.iterator
    val iteOfMetadata = arrayOfMetadata.iterator

    //initialised variable for Iterator[Row]
    var curObj:AnyRef = iteOfObjs.next()
    var curMetadata = iteOfMetadata.next()

    new Iterator[Row] {
      override def hasNext: Boolean = {
        if(curMetadata==null){
          false
        }
        else {      //uses dataloader to check iterator
          val classInst = Class.forName(curMetadata).newInstance().asInstanceOf[getAndConvert]
          classInst.hasNextT(curObj) match {
            case true => true
            case false =>
              Try(curObj = iteOfObjs.next()) match {        //Try can skip pass empty iterator to return false when required
                case Success(y) =>
                  curMetadata = iteOfMetadata.next()
                  val classInst = Class.forName(curMetadata).newInstance().asInstanceOf[getAndConvert]
                  classInst.hasNextT(curObj)
                case Failure(x) =>
                  false
              }
          }
        }
      }

      override def next(): Row = {
        val row = getRow(curObj, curMetadata)

        if(row != Row.empty) {                             //mapping which is required
          val values = columns.map {
            case "Date" => Date.valueOf(row.getString(0))
            case "Time" => row.getString(1)
            case "Action" => row.getString(2)
            case "Role" => row.getString(3)
            case "ActionID" => row.getInt(4)
            case "Description" => row.getString(5)
          }
          Row.fromSeq(values)
        }
        else {
          Try(curObj = iteOfObjs.next()) match {     //Try can skip pass empty iterator to return false when required
            /*Success accounts for the start of reading the next iterator if it happens that the current iterator is of the type
            that only know it has finished when it returns nothing. In this implementation, datasourceCRelation is the example
            of this. If the last file read is of this type of iterator, the last row will be null everytime.               */
            case Success(y) =>
              curMetadata = iteOfMetadata.next()
              val nextRow = getRow(curObj, curMetadata)
              val values = columns.map {
                case "Date" => Date.valueOf(nextRow.getString(0))
                case "Time" => nextRow.getString(1)
                case "Action" => nextRow.getString(2)
                case "Role" => nextRow.getString(3)
                case "ActionID" => nextRow.getInt(4)
                case "Description" => nextRow.getString(5)
              }
              Row.fromSeq(values)
            case Failure(x) => {
              curMetadata = null
              Row(null, null, null, null, null, null)
            }
          }
        }
      }
    }
  }

  override protected def getPartitions: Array[Partition] = Array(partitionsV3RC(0))
}

case class partitionsV3RC(idx: Int) extends Partition {
  override def index: Int = idx
}