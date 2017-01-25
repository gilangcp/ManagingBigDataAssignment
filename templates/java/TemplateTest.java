package nl.utwente.bigdata;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.Comparator;
import java.util.Collections;
import java.io.Serializable;
import java.io.IOException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.junit.Test;
import org.junit.Before;

import scala.Tuple2;

public class ${toolName}Test implements Serializable {
    private static final long serialVersionUID = -5681683598336701496L;
    
    private JavaSparkContext sc;

    @Before
    public void init() throws IllegalArgumentException, IOException {
        this.sc = TestUtils.getSparkContext();
    }
    
    @Test
    public void testCount() {
      List<String> input = Arrays.asList("Heart", "Diamonds Heart");
      JavaRDD<String> inputRDD = sc.parallelize(input);

      $toolName javaWordCount = new $toolName();
      
      // calculate output (adapt according to return type)
      JavaRDD<String> result = javaWordCount.doJob(inputRDD);
      List<String> resultList = result.collect();

      // retrun expected output
      List<String> expectedOutput = Arrays.asList("Heart", "Diamonds Heart");

      // output (adapt according to result type)
      assert TestUtils.listEquals(resultList, expectedOutput, new TestUtils.ValueComparator<String>());
    }
}