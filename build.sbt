name := "CustomDatasourceYY"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "io.minio" % "minio" % "7.0.2"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.5"
//libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.7.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.7.3"
//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3"
//libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.3"
//libraryDependencies += "joda-time" % "joda-time" % "2.9.9"
//libraryDependencies += "commons-io" % "commons-io" % "2.4"
//libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.3"

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.6.7"
  )
}