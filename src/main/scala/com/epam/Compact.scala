package com.epam

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConversions._

object Compact extends App {

  override def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Compaction")
      .getOrCreate()


    //    val file1 = "hdfs://sandbox-hdp.hortonworks.com:8020/topics/scala_confluent/year=2020/month=08/day=11/hour=01/scala_confluent+0+0031828531+0031832242.avro"
    //    val f1Path = new Path(file1)
    //    val file2 = "hdfs://sandbox-hdp.hortonworks.com:8020/topics/scala_confluent/year=2020/month=08/day=11/hour=01/scala_confluent+0+0031832243+0031858690.avro"
    //    val f2Path = new Path(file2)

    val path = args(0)
    //    val path1 = args(1)
    //    val outputPath = args(1)
    val conf = new Configuration()
    conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    //    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val fs = FileSystem.get(URI.create(path), conf)
    //    val status = fs.getFileStatus(new Path(path))
    //    val statusF1_before = fs.getFileLinkStatus(f1Path)
    //    println("File1 Size(KBs) before= " + statusF1_before.getLen / 1024)
    //    fs.concat(f1Path, Array[Path] {f2Path})
    //    val statusF1_after = fs.getFileLinkStatus(f1Path)
    //    println("File1 Size(KBs) after= " + statusF1_after.getLen / 1024)

    //    val avroFiles = spark.read.format("com.databricks.spark.avro").load(path)
    //    avroFiles.show(3)

    val pathsList = getPartitionPathList(fs, new Path(path))
    println("--------------->PathList= "+pathsList)

//    val s = "hdfs://sandbox-hdp.hortonworks.com:8020/topics/scala_confluent/year=2020/month=08/day=17"
    //    println(listFiles(s, fs))
    for (e <- pathsList) {
      println("---------------->Path= " + e.toString)
      val avroFiles = spark.read.format("com.databricks.spark.avro").load(e.toString)
      println("---------------->Count= " + avroFiles.count())
//      var year = ""
//      var month = ""
//      var day = ""
//      val arr = e.toString.split('/')
//      for (a <- arr) {
//        if (a.contains("year")) {
//          year = a
//        }
//        if (a.contains("month")) {
//          month = a
//        }
//        if (a.contains("day")) {
//          day = a
//        }
//      }
      //      val fullMame = year + "|" + month + "|" + day
      //      val fullOutputPath = path + "/" + year + "/" + month + "/" + day + "_compacted"
//      val fullOutputPath = e
      fs.delete(e, true)
      spark.catalog.refreshByPath(e.toString)
      avroFiles
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("com.databricks.spark.avro")
        .save(e.toString)
    }

    //    println("Path= " + status.getPath)
    //    println("---------------")
    //    println("File Size(KBs)= " + status.getLen / 1024)
    //    println("---------------")
    //    println("Block Size(KBs)= " + status.getBlockSize / 1024)
    //    println("---------------")
    //    println(status.getPermission)
    //    println("---------------")
    //    println("Is Dir= " + status.isDirectory)
    //    println("---------------")
    //    println(println("getUsed Path MB =" + fs.getUsed(new Path(path)) / (1024 * 1024)))
    //    println("ListStatus " + fs.listStatus(new Path(path)).mkString("Array(", ", ", ")"))

    //    println(println("getUsed Path MB =" + fs.getUsed(new Path(path)) / (1024 * 1024)))
    //    println(println("getUsed outputPath MB = " + fs.getUsed(new Path(outputPath)) / (1024 * 1024)))

  }

  import java.util

  @throws[Exception]
  protected def getPartitionPathList(fs: FileSystem, path: Path): util.ArrayList[Path] = {
    val files = fs.listStatus(path)
    val paths = new util.ArrayList[Path]
    if (files != null) for (eachFile <- files) {
      if (eachFile.isFile) {
        paths.add(path)
        return paths
      }
      else paths.addAll(getPartitionPathList(fs, eachFile.getPath))
    }
    paths
  }

  import java.io.IOException

  import org.apache.hadoop.fs.FileSystem

//  @throws[IOException]
//  protected def listFiles(hdfsDirPath: String, fileSystem: FileSystem): util.ArrayList[String] = {
//    //    var files = ""
//    val files = new util.ArrayList[String]
//    val path = new Path(hdfsDirPath)
//    //    val fileSystem = FileSystem.get(conf)
//    //    if ("files" == content) {
//    val iterator = fileSystem.listFiles(path, false)
//    while (iterator.hasNext) {
//      files.add(iterator.next.getPath + iterator.next.getPath.getName)
//    }
    //    {
    //      val status = fileSystem.listStatus(path)
    //      for (i <- 0 until status.length) {
    //        if (status(i).isDirectory) files = files + status(i).getPath.getName + "/\n"
    //        else files = files + status(i).getPath.getName + "\n"
    //      }
    //    }
//    files
//  }

}
