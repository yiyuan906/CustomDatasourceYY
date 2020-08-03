import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val endpoint = "http://127.0.0.1:9000"
    val access_key = "minio"
    val secret_key = "minio123"

    val Spark = SparkSession.builder()
      .master("local[*]")
      .appName("Metadata read to schema example")
      .getOrCreate()

    //v2 read
    val loadOne = Spark.read.format("ySource2")
      .option("endpoint",endpoint)
      .option("accesskey",access_key)
      .option("secretkey",secret_key)
      .load("customdatasources/v2/")
    loadOne.show(30)

    //v3 read
    val loadTwo = Spark.read.format("ySource3")
      .option("endpoint",endpoint)
      .option("accesskey",access_key)
      .option("secretkey",secret_key)
      .load("customdatasources/v3/")
    loadTwo.show(30)

    //alternative read used to test different order of files
    val loadThree = Spark.read.format("ySource3")
      .option("endpoint",endpoint)
      .option("accesskey",access_key)
      .option("secretkey",secret_key)
      .option("readMode", "specific")
      .load("customdatasources/v3/logA1.txt, customdatasources/v3/logC1.parquet, customdatasources/v3/logB1.csv")
    loadThree.show()
  }
}
