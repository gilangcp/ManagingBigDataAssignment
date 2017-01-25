package nl.utwente.bigdata; // don't change package name

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import scala.language.dynamics
import util.parsing.json.JSON
import scala.io.Source
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.Matcher;
import java.util.Locale;
// uncomment if your program uses sql
//import org.apache.spark.sql.{ SQLContext }

object CountKeywordByUniqueUserPerMonth {

 /* 
   add actual program here, start by specifying
   the input and output types in RDD[X]
   */
  def doJob(input:RDD[String], keyword: String) : RDD[(String,Int)] = {

    input.filter(data => {data.toLowerCase().contains(keyword)})
      .map( data => {
          var userid : String = ""
          var date :String  = "null"

          try{
            userid = JsonElement.parse(data).get.user.id
            var format: DateTimeFormatter =  DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY").withLocale(Locale.ENGLISH);
            val dateTime : DateTime = format.parseDateTime(JsonElement.parse(data).get.created_at)

            date = dateTime.toString("M-YYYY")

          }catch{
            case e: Exception => {
          }
        }
        ( (userid, date ) ,1)
      })
      .reduceByKey((a,b) => 1)
      .map(data => {
        (data._1._2, 1)
      }).reduceByKey((a,b)=> {a+b})
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



//Parser JSON
trait JsonElement extends Dynamic{ self =>
  def selectDynamic(field: String) : JsonElement = EmptyElement
  def applyDynamic(field: String)(i: Int) : JsonElement = EmptyElement
  def toList : List[String] = sys.error(s"$this is not a list.")
  def asString: String = sys.error(s"$this has no string representation.")
  def length$ : Int = sys.error(s"$this has no length")
}


object JsonElement{

  def ^(s: String) = {
    require(!s.isEmpty, "Element is empty")
    s
  }

  implicit def toString(e: JsonElement) : String = e.asString
  implicit def toBoolean(e: JsonElement) : Boolean = (^(e.asString)).toBoolean
  implicit def toBigDecimal(e: JsonElement) : BigDecimal = BigDecimal(^(e.asString))
  implicit def toDouble(e: JsonElement) : Double = ^(e.asString).toDouble
  implicit def toFloat(e: JsonElement) : Float = ^(e.asString).toFloat
  implicit def toByte(e: JsonElement) : Byte = ^(e.asString).stripSuffix(".0").toByte
  implicit def toShort(e: JsonElement) : Short = ^(e.asString).stripSuffix(".0").toShort
  implicit def toInt(e: JsonElement) : Int = ^(e.asString).stripSuffix(".0").toInt
  implicit def toLong(e: JsonElement) : Long = ^(e.asString).stripSuffix(".0").toLong
  implicit def toList(e: JsonElement) : List[String] = e.toList



  def parse(json: String) = JSON.parseFull(json) map (JsonElement(_))

  def apply(any : Any) : JsonElement = any match {
    case x : Seq[Any] => new ArrayElement(x)
    case x : Map[String, Any] => new ComplexElement(x)
    case x => new PrimitiveElement(x)
  }
}

case class PrimitiveElement(x: Any) extends JsonElement{
  override def asString = x.toString
}

case object EmptyElement extends JsonElement{
  override def asString = ""
  override def toList = Nil
}

case class ArrayElement(private val x: Seq[Any]) extends JsonElement{
  private lazy val elements = x.map((JsonElement(_))).toArray

  override def applyDynamic(field: String)(i: Int) : JsonElement = elements.lift(i).getOrElse(EmptyElement)
  override def toList : List[String] = elements map (_.asString) toList
  override def length$ : Int = elements.length
}

case class ComplexElement(private val fields : Map[String, Any]) extends JsonElement{
  override def selectDynamic(field: String) : JsonElement = fields.get(field) map(JsonElement(_)) getOrElse(EmptyElement)
}
