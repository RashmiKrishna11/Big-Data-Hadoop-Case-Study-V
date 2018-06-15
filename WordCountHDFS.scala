package com.acadgild.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.{FileAlreadyExistsException, FileSystem, FileUtil, Path}
import scala.io.Source
object WordCountHDFS {
 def main(args: Array[String]) {
  
 //Create conf object
 val conf = new SparkConf().setMaster("local[*]")
 .setAppName("WordCount")
  //create spark context object
 val sc = new SparkContext(conf)
val hadoopConf = new Configuration()
//Check whether sufficient params are supplied
 if (args.length < 2) {
 println("Usage: ScalaWordCount<output1> <output2>")
 System.exit(1)
 }
 //Read file and create RDD
 val rawData = sc.textFile("/home/acadgild/wordcount/")
 hadoopConf.addResource(new Path("/home/acadgild/install/hadoop/hadoop-2.6.5/etc/hadoop/core-site.xml"))
    hadoopConf.addResource(new Path("/home/acadgild/install/hadoop/hadoop-2.6.5/etc/hadoop/hdfs-site.xml"))
 val fs = FileSystem.get(hadoopConf);
        val sourcePath = new Path("/home/acadgild/wordcount/");
        val destPath = new Path("hdfs://localhost:8020/");
        if(!(fs.exists(destPath)))
        {
            System.out.println("No Such destination exists :"+destPath);
            return;
        }
         
        fs.copyFromLocalFile(sourcePath, destPath);
          //convert the lines into words using flatMap operation
         val words = rawData.flatMap(line => line.split(" "))
          val hdfsfile = sc.textFile("hdfs://localhost:8020/wordcount/test")
         val hdfswords = hdfsfile.flatMap(line => line.split(" "))
         //count the individual words using map and reduceByKey operation
          
         val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)
         val hdfsWC = hdfswords.map(word => (word,1)).reduceByKey(_ + _)
         //Save the result
          wordCount.saveAsTextFile(args(0))
          hdfsWC.saveAsTextFile(args(1))
         val LFSWCfile = Source.fromFile("/home/acadgild/wordcount1/part-00000").getLines().toArray
         val hdfsWCfile= Source.fromFile("/home/acadgild/wordcount2/part-00000").getLines().toArray
         val elem = LFSWCfile.sameElements(hdfsWCfile)
         if(elem == false){
           println("Error!: Contents mismatch")
         }else
           println("Contents match!")
         
         wordCount.collect().foreach(print)
         
         hdfsWC.collect().foreach(print)
        //stop the spark context
         sc.stop
 
 }
}