package nl.utwente.bigdata;
 
import org.apache.spark.rdd.RDD  
import org.apache.spark.{SparkConf, SparkContext}  
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class ${toolName}Test extends FlatSpec with Matchers with BeforeAndAfter {

  var sc:SparkContext = _

  before {
    // create spark context
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test")
    sc = new SparkContext(sparkConf)
  }

  after {
    sc.stop()
  }

  /* write similar test cases as the one above
  
  behavior of "Test"

  it should "count words in a text" in {
    // prepare input and expected input
    val text =
      """Hello Spark
        |Hello world
      """.stripMargin
    // convert input into rdd
    val input: RDD[String] = sc.parallelize(List(text))
    // calculate output
    val output: RDD[(String, Int)] = $toolName.doJob(input)
    // check output
    output.collect() should contain allOf (("Hello", 2), ("Spark", 1), ("world", 1))
  }
  */
}