package com.acadgild.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
 
object Wordcount {
 def main(args: Array[String]) {
  
 //Create conf object
 val conf = new SparkConf().setMaster("local[*]")
 .setAppName("WordCount")
  
 //create spark context object
 val sc = new SparkContext(conf)
 
//Check whether sufficient params are supplied
 if (args.length < 1) {
 println("Usage: ScalaWordCount <input> <output>")
 System.exit(1)
 }
 //Read file and create RDD
 val rawData = sc.textFile("/home/acadgild/wordcount")
  
 //convert the lines into words using flatMap operation
 val words = rawData.flatMap(line => line.split(" "))
  
 //count the individual words using map and reduceByKey operation
 val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)
  
 //Save the result
 wordCount.saveAsTextFile(args(0))
 
//stop the spark context
 sc.stop
 }
}