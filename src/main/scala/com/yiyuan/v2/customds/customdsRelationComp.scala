package com.yiyuan.v2.customds

import java.sql.Date

import io.minio.MinioClient
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
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

  //get from dataloader a string value to determine how the file is read
  def getReadType(metadata:String,objName:String):String = {
    Try(Class.forName(metadata)) match {
      case Success(classInstance) =>
        val method = classInstance.getDeclaredMethod("wayOfReading")
        method.invoke(classInstance.newInstance()).asInstanceOf[String]
      case Failure(x) => //way of reading not specified
        if(objName.endsWith(".parquet"))
          "parquet"
        else "text"     //accounts for csv and text
    }
  }

  //Checks for objects in directory or all objects specified
  def getObjList(streamPath:String, minio:MinioClient):Array[String] = {
    if(readMode == "specific"){           //expects list to be given seperated by commas "bucket/dir/obj,bucket/dir/obj"
      streamPath.split(",").map{path=>
        path.drop(bucket.size+1)
      }
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

  def getStream(streamPath: String, minio:MinioClient, readType:String):Any = {
    if (!minio.bucketExists(bucket))
      throw new Exception("Bucket does not exist")

    readType match {
      case "parquet" =>
        val hConf = new Configuration()
        hConf.set("fs.s3a.endpoint", endpoint)
        hConf.set("fs.s3a.access.key", accesskey)
        hConf.set("fs.s3a.secret.key", secretkey)
        val path = new Path(s"s3a://$bucket/$streamPath")
        val parReader = ParquetReader.builder(new GroupReadSupport(),path).withConf(hConf).build()
        parReader
      case _ =>                           //txt and csv
        val minioStream = minio.getObject(bucket,streamPath)
        if(streamPath.endsWith("csv")) {
          val Ite = IOUtils.readLines(minioStream,"UTF-8").iterator()
          Ite.next()
          Ite
        }
        else
          IOUtils.readLines(minioStream,"UTF-8").iterator()
    }
  }

  //Get method from dataloader to transform line
  def getMethod(metadata:String,readType:String,obj:Any):java.lang.reflect.Method = {
    readType match {
      case "parquet" =>
        val phObj = obj.asInstanceOf[ParquetReader[Group]]
        Try(Class.forName(metadata)) match {
          case Success(classInstance) =>
            classInstance.getDeclaredMethod("singleStream", phObj.getClass)
          case Failure(x) =>
            throw new Exception(s"Class $metadata cannot be found")
        }
      case _ =>
        Try(Class.forName(metadata)) match {
          case Success(classInstance) =>
            classInstance.getDeclaredMethod ("streamParsing", new String().getClass)
          case Failure(x) =>
            throw new Exception(s"Class $metadata cannot be found")
        }
    }
  }

  //transformation of line to row
  def getRow(currentObject:Any, currentMetadata:String,readType:String , currentMethod:java.lang.reflect.Method):Row = {
    readType match {
      case "parquet" =>
        val parStream = currentObject.asInstanceOf[ParquetReader[Group]]
        Try(currentMethod.invoke(Class.forName(currentMetadata).newInstance(), parStream).asInstanceOf[Row]) match {
          case Success(row) => row
          case Failure(x) => Row.empty
        }
      case _ =>
        val line = currentObject.asInstanceOf[java.util.Iterator[String]]
        Try(currentMethod.invoke(Class.forName(currentMetadata).newInstance(),line.next()).asInstanceOf[Row]) match {
          case Success(row) => row
          case Failure(x) => Row.empty
        }
    }
  }

  var metadataRef = new ArrayBuffer[(String,String)]

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val minioObj = new MinioClient(endpoint, accesskey, secretkey)
    val listOfObj = getObjList(compPath, minioObj)

    //getting Array(dataIterator,metadata,readType)
    val fullDetailsOfObjs = listOfObj.map { ObjName =>
      val metadata = getMetadata(ObjName,minioObj)
      val readType = getReadType(metadata,ObjName)
      (getStream(ObjName, minioObj,readType), metadata, readType)
    }

    val listOfObjIte = listOfObj.iterator

    println("\nObjects excluded from read:")
    println("="*30)
    val filteredFullDetailsOfObjs = fullDetailsOfObjs.filter(x=> {        //filter objects with empty metadata
      val currentObjectName = listOfObjIte.next()
      if (x._2.contains("Empty")) {       //"Empty" gotten when nothing read from metadata
        println(currentObjectName)
        false
      }
      else
        true
    })
    println("="*30)

    val (arrayOfObjs,arrayOfMetadata,arrayOfreadType) = filteredFullDetailsOfObjs.unzip3

    val arrayOfMethod = filteredFullDetailsOfObjs.map(x=>{
      getMethod(x._2,x._3,x._1)
    })

    val iteOfObjs = arrayOfObjs.iterator
    val iteOfMetadata = arrayOfMetadata.iterator
    val iteOfreadType = arrayOfreadType.iterator
    val iteOfMethods = arrayOfMethod.iterator

    //initialised variable for Iterator[Row]
    var curMethod = iteOfMethods.next()
    var curReadType = iteOfreadType.next()
    var curObj:Any = iteOfObjs.next()
    var curMetadata = iteOfMetadata.next()

    new Iterator[Row] {
      override def hasNext: Boolean = {
        if (curReadType != null) {
          curReadType match {
            case "parquet" =>       //logic handled in next() for parquet
              true
            case _ =>
              if(curObj.asInstanceOf[java.util.Iterator[String]].hasNext)
                true
              else
                iteOfObjs.hasNext
          }
        }
        else
          false
      }

      override def next(): Row = {
        val row = getRow(curObj, curMetadata, curReadType, curMethod)

        if(row != Row.empty) {
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
          Try(curObj = iteOfObjs.next()) match {
            /*Success accounts for the start of reading the next iterator if it happens that the current iterator is of the type
            that only know it has finished when it returns nothing. In this implementation, datasourceCRelation is the example
            of this. If the last file read is of this type of iterator, the last row will be null everytime.               */
            case Success(y) =>
              curMetadata = iteOfMetadata.next()
              curReadType = iteOfreadType.next()
              curMethod = iteOfMethods.next()
              val nextRow = getRow(curObj, curMetadata, curReadType, curMethod)
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
              curReadType = null
              Row(null, null, null, null, null, null)
            }
          }
        }
      }
    }
  }

  override protected def getPartitions: Array[Partition] = Array(partitionsV2RC(0))
}

case class partitionsV2RC(idx: Int) extends Partition {
  override def index: Int = idx
}