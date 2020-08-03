# CustomDatasourceYY

This implementation is a custom datasource that solves the issue of requiring to manually format web application log files of different
schema and format to combine and get the data wanted altogether from an object store. 
This is done by making use of dataloaders which can be added in the custom datasource to format web application log files data to the
standardised schema that is wanted at the output. 
The use of additional metadata that can be added to objects stored in an object store is the core of this implementation.
The additional metadata in this case contains the classpath to the designated dataloader, the custom datasource would then be able to make
use of this metadata to use the dataloader to parse the file accordingly.

## Using the custom datasource 

This custom datasource can be used by specifying `spark.read.format("ySource3").option("endpoint",endpoint).option("accesskey",accesskey).option("secretkey",secretkey)`.
 Where `ySource3` is the name of the custom datasource and the options containing the credentials to the MinIO server.
The `.load(path)` can then be used to load the data wanted, the path it is looking for would look like `bucket/dir` or `bucket/dir/object`.

There is a option to specify a list of objects that can be read by adding `.option("readMode","specific")`. 
The path its going to be looking for in this case would be `bucket/dir/file1.txt,bucket/dir/file2.csv,bucket/file3.parquet` where the 
files will be read in that specified order.

The current custom datasource contains txt, csv and parquet dataloaders which can be referenced to extend to new dataloaders for the 
custom datasource.

## Setup required

The files used for this example can be found in the data and parquetfile directory. 
The name of the files indicates their respective dataloaders, the dataloader for logA1.txt is datasourceARelation. 
`fileupload.txt` can be used as reference if unclear.

- MinIO
- Spark
- IntelliJ IDEA
- sbt
- Setting of metadata

### MinIO server setup
Install MinIO Server from [here](https://docs.min.io/docs/minio-quickstart-guide).

After installing, a local MinIO server can be started up by simply doing 
`MINIO_ACCESS_KEY=accesskey MINIO_SECRET_KEY=secretkey ./minio server /path/to/local/miniostorage`.
To check, go to https://127.0.0.1:9000 and enter the access key and secret key to enter.

Now download the MinIO client from [here](https://docs.min.io/docs/minio-client-quickstart-guide).
By running `mc config host add myminio https://10.0.2.15:9000 accesskey secretkey`,  the local MinIO server that was just started up
has now been identified by the MinIO client as `myminio`. Now the MinIO client can aid in uploading files to the MinIO server. 
(Scripts can also be used to upload objects to MinIO through the use of MinIO library)


### Spark setup (The one used for this implementation)
Download Apache Spark version `spark-2.4.5-bin-hadoop2.7.tgz` from [here](https://archive.apache.org/dist/spark/spark-2.4.5/).

The dependencies required are:
  - [`Hadoop AWS 2.7.3`](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.3)
  - [`AWS SDK For Java 1.7.4`](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.7.4)
  - [`Simple XML 2.7.1`](https://mvnrepository.com/artifact/org.simpleframework/simple-xml/2.7.1)
  - [`Minio 7.0.2`](https://mvnrepository.com/artifact/io.minio/minio/7.0.2)
  - [`Guava: Google Core Libraries For Java 28.2-jre`](https://mvnrepository.com/artifact/com.google.guava/guava/28.2-jre)

Extract the `spark-2.4.5-bin-hadoop2.7.tgz` tar file to the directory where Spark is suppose to be installed. 
Afterwards, move all the dependency jar files downloaded previously into `/path/to/spark-2.4.5-bin-hadoop2.7/jars` directory.

### IntelliJ IDEA
Get the scala and sbt plugin on IntelliJ IDEA.
This file can be opened by IntelliJ IDEA after using `git clone` to download it. Reload all sbt projects and it should be good to use.

### sbt 
Run `sbt package` on this file's directory (dir/CustomDatasourceYY/) to get a compiled jar file of this program. 
The jar file made under the target directory of this file can then be used in Spark 

### Setting of metadata
The metadata can be set by using `setfattr -n key1 -v value1 filename`.
The MinIO client is used to copy the file over to the server, directly uploading it would result in the uploaded file not having the
set metadata. `mc cp filename myminio/bucket-name/filename`

Another alternative is to copy the file to MinIO and write the metadata by using 
`mc cp --attr key1=value1 filename myminio/bucket-name/filename`. The file `fileupload.txt` can be referenced for this. 

Scripts can also be used to upload objects with metadata, [this](https://stackoverflow.com/questions/49230684/metadata-on-minio-object-storage) 
post can be used as reference for uploading an object with metadata.

## Running the example test

To run the Test file, it only needs the objects to be setup with the necessary metadata in MinIO and the file being ran on IntelliJ IDEA.
Make necessary changes to the paths read if needed.

## Using it on Spark-shell

To run it on Spark-shell, Spark has to be installed with the necessary dependencies and also have the objects on MinIO containing 
the necessary metadata. (All found in "Setup Required")
Use the jar file made from `sbt package` and put it in the jar file of the previously installed Spark at
`/path/to/spark-2.4.5-bin-hadoop2.7/jars`.
What is in the Test file can be used on the spark-shell for testing.
