package nl.utwente.bigdata; // don't change package name

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD

// uncomment if your program uses sql
//import org.apache.spark.sql.{ SQLContext }

object $toolName {

  /* 
   add actual program here, start by specifying
   the input and output types in RDD[X]
   */
  def doJob(input: RDD[String]) : RDD[String] = {
      input
  }

  def main(args: Array[String]) {
    // command line arguments
    val appName = this.getClass.getName
    
    // interpret command line, default: first argument is input second is output
    val inputDir = args(0)
    val outputDir = args(1)

    // configuration
    val conf = new SparkConf()
      .setAppName(s"$appName $inputDir $outputDir")

    // create spark context
    val sc = new SparkContext(conf)
    // uncomment if your program uses sql
    // val sqlContext = new SQLContext(sc)

    // potentially 
    doJob(sc.textFile(inputDir)).saveAsTextFile(outputDir)
    
  }
}