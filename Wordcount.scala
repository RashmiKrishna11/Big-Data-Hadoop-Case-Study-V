package SparkstreamingAssignments

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
object Wordcount {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("TextFileStream").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val lines = ssc.textFileStream("e:\\sparkstreaming")
    val wordcounts = lines.flatMap(line => line.split(" ").map(word =>(word,1))).reduceByKey(_ + _)
    wordcounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
