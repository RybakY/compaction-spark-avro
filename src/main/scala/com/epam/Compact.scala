package com.epam

import java.net.URI
import java.util

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

    val inputPath = args(0)
    val conf = new Configuration()
    conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    val fileSystem = FileSystem.get(URI.create(inputPath), conf)

    val partitionsList = getPartitionPathList(fileSystem, new Path(inputPath))
    for (s <- partitionsList) {
      val n = countNumberOfFiles(conf, s)
      println("---------------->Before filter Number files in dir [" + s + "]: " + n)
    }
    //    partitionsList.filter(p => countNumberOfFiles(conf, p) > 4)
    val partitionsListFiltered = new util.ArrayList[Path]
    for (s <- partitionsList) {
      if (countNumberOfFiles(conf, s) > 3) {
        partitionsListFiltered.add(s)
      }
    }
    for (s <- partitionsListFiltered) {
      val n = countNumberOfFiles(conf, s)
      println("---------------->After filter Number files in dir [" + s + "]: " + n)
    }
    println("--------------->PathList= " + partitionsList)
    println("--------------->PathListFiltered= " + partitionsListFiltered)

    for (p <- partitionsList) {
      val avroFiles = spark
        .read
        .format("com.databricks.spark.avro")
        .load(p.toString)
      //      avroFiles.cache.count()
      //      val catalyst_plan = avroFiles.queryExecution.logical
      //      val df_size_in_bytes = spark.sessionState.executePlan(
      //        catalyst_plan).optimizedPlan.stats.sizeInBytes
      //      println("------------------> Size(MBs)= " + df_size_in_bytes / (1024 * 1024))

      val tmpOutputPath = p + "_tmp"
      avroFiles
        .coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("com.databricks.spark.avro")
        .save(tmpOutputPath)

      fileSystem.delete(p, true)
      fileSystem.rename(new Path(tmpOutputPath), p)

    }
    //    val pathsListAfter = getPartitionPathList(fs, new Path(path))
    //    val avroFilesAfter0 = spark.read.format("com.databricks.spark.avro").load(pathsListAfter.get(0).toString)
    //    println("---------------->Count After 0= " + avroFilesAfter0.count())
    //    val avroFilesAfter1 = spark.read.format("com.databricks.spark.avro").load(pathsListAfter.get(1).toString)
    //    println("---------------->Count After 1= " + avroFilesAfter1.count())

    //    println("Path= " + status.getPath)
    //    println("File Size(KBs)= " + status.getLen / 1024)
    //    println("Block Size(KBs)= " + status.getBlockSize / 1024)
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

  protected def countNumberOfFiles(conf: Configuration, path: Path): Int = {
    val fileSystem = FileSystem.get(URI.create(path.toString), conf)
    var numberFiles = 0
    val status = fileSystem.listStatus(path)
    status
      .filter(d => d.isFile)
      .foreach(_ => numberFiles = numberFiles + 1)
    numberFiles
  }

}
