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
    println("--------------->PathList= " + pathsList)

    //    val s = "hdfs://sandbox-hdp.hortonworks.com:8020/topics/scala_confluent/year=2020/month=08/day=17"
    //    println(listFiles(s, fs))
    for (p <- pathsList) {
//      println("---------------->Path= " + p.toString)
      val avroFiles = spark.read.format("com.databricks.spark.avro").load(p.toString)
//      println("---------------->Count Before= " + avroFiles.count())
      var numberFiles = 0
      val fs1 = FileSystem.get(URI.create(p.toString), conf)
      val status = fs1.listStatus(p)
      println("----------Files in dir [" + p + "]:")
      status.foreach(x => println(x.getPath))
      status.filter(d => d.isFile).foreach(_ => numberFiles = numberFiles + 1)
      println("--------------Number of files in dir [" + p + "]: " + numberFiles)

      //      println("------------->List files (true)" + fs.listFiles(p, true))
      //      println("------------->List files (false)" + fs.listFiles(p, false))
      //      while (fs.listFiles(p, false).hasNext) {
      //        numberFiles = numberFiles + 1
      //      }
      //      println("--------------->Number files of " + p + ": " + numberFiles)
      avroFiles.cache.count()
      val catalyst_plan = avroFiles.queryExecution.logical
      val df_size_in_bytes = spark.sessionState.executePlan(
        catalyst_plan).optimizedPlan.stats.sizeInBytes
      println("------------------> Size(MBs)= " + df_size_in_bytes / (1024 * 1024))

      val tmpOutputPath = p + "_tmp"
      //      spark.catalog.refreshByPath(e.toString)
      avroFiles
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("com.databricks.spark.avro")
        .save(tmpOutputPath)

      fs.delete(p, true)
      fs.rename(new Path(tmpOutputPath), p)

    }
    //    val pathsListAfter = getPartitionPathList(fs, new Path(path))
    //    val avroFilesAfter0 = spark.read.format("com.databricks.spark.avro").load(pathsListAfter.get(0).toString)
    //    println("---------------->Count After 0= " + avroFilesAfter0.count())
    //    val avroFilesAfter1 = spark.read.format("com.databricks.spark.avro").load(pathsListAfter.get(1).toString)
    //    println("---------------->Count After 1= " + avroFilesAfter1.count())

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
