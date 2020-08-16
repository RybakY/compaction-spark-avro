package com.epam

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object Compact extends App {


  private var codecFactory = null
  //  private[spark_compaction] var fs = null
  //  private[spark_compaction] var fsArray = null

  import java.net.URI

  import org.apache.hadoop.fs.FileSystem


  override def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local")
      //      .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .appName("Spark Compaction")
      .getOrCreate()


    //    val path="hdfs://localhost:8020/topics/scala_confluent/year=2020/month=08/day=11/hour=01/scala_confluent+0+0032017843+0032044290.avro"
    val path = args(0)
    val conf = new Configuration()
    conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    //    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val fs = FileSystem.get(URI.create(path), conf)
    val status = fs.getFileStatus(new Path(path))


    val avroFiles = spark.read.format("com.databricks.spark.avro").load(path)
    avroFiles.show(3)

    println("File Size(KBites)= "+status.getLen/1024)
    println("Block Size(KBites)= "+status.getBlockSize/1024)
    println(status.getPermission)
    println("Is Dir= " + status.isDirectory)


  }


}
