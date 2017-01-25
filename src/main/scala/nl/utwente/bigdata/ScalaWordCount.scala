package nl.utwente.bigdata;

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD

object ScalaWordCount {

  /* 
     function that takes a list of texts and returns the counts
     of individual words 
  */
  def count(lines: RDD[String]) : RDD[(String,Int)] = {
    val counts = lines.flatMap(line => line.split("\\W+"))
      .map(word => (word, 1))
      .reduceByKey((a,b) => a+b)
    counts
  }

  /*
   Script to load text documents from disk and store the counts
   in a set of textfiles
   */
  def main(args: Array[String]) {
    // command line arguments
    val appName = this.getClass.getName

    // interpret command line, default: first argument is input second is output
    val inputDir = args(0)
    val outputDir = args(1)

    // configuration
    val conf = new SparkConf()
      .setAppName(s"$appName $inputDir $outputDir")
    // spark context
    val sc = new SparkContext(conf)

    count(sc.textFile(inputDir)).saveAsTextFile(outputDir)
  }
}