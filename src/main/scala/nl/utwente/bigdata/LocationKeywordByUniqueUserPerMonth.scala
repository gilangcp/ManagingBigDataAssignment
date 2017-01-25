package nl.utwente.bigdata; // don't change package name

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import scala.language.dynamics
import util.parsing.json.JSON
import scala.io.Source
import org.joda.time._
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Locale;

// uncomment if your program uses sql
//import org.apache.spark.sql.{ SQLContext }

object LocationKeywordByUniqueUserPerMonth {

  /* 
   add actual program here, start by specifying
   the input and output types in RDD[X]
   */
  def doJob(input:RDD[String], keyword : String) : RDD[(String,Int)]= {
          input
      .filter(data=> {
          data.toLowerCase().contains(keyword)
      })
      .map( data => {
          var userid : String = ""
          var location : String = ""
          var date :String  = "-1"

          try{
            var parsed : Option[nl.utwente.bigdata.JsonElement] = JsonElement.parse(data)

            userid = parsed.get.user.id
            location = parsed.get.user.location

            var format: DateTimeFormatter =  DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY").withLocale(Locale.ENGLISH);
            val dateTime : DateTime = format.parseDateTime(JsonElement.parse(data).get.created_at)
            date = dateTime.toString("MM-YYYY")

          }catch{
            case e: Exception => {

          }
        }
        ((userid,location,date),1)
      })
      .reduceByKey((a,b) => 1)
      .map(data => {

        ( data._1._2 + ","+data._1._3 , 1)
      })
      .reduceByKey((a,b)=> a+b)
  }

  def main(args: Array[String]) {
    // command line arguments
    val appName = this.getClass.getName
    
    // interpret command line, default: first argument is input second is output
    val keyword = args(0)
    val inputDir = args(1)
    val outputDir = args(2)

    // configuration
    val conf = new SparkConf()
      .setAppName(s"$appName $inputDir $outputDir")

    // create spark context
    val sc = new SparkContext(conf)
    // uncomment if your program uses sql
    // val sqlContext = new SQLContext(sc)

    // potentially 
    doJob(sc.textFile(inputDir),keyword).repartition(1).saveAsTextFile(outputDir)
    
  }
}