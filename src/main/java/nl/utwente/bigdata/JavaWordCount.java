/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.utwente.bigdata;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import java.io.Serializable;

public final class JavaWordCount implements Serializable {
  private static final long serialVersionUID = -2312231231L;
  
  private static final Pattern SPACE = Pattern.compile(" +");

  
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: JavaWordCount <input> <output>");
      System.exit(1);
    }
    
    String input_dir = args[0];
    String output_dir = args[1];

    SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(input_dir, 1);

    JavaWordCount instance = new JavaWordCount();
    JavaPairRDD<String, Integer> result = instance.doJob(lines);

    result.saveAsTextFile(output_dir);

    ctx.stop();
  }
  
  public JavaPairRDD<String, Integer> doJob(JavaRDD<String> lines) {
    JavaRDD<String> words = lines
        .flatMap(new FlatMapFunction<String, String>() {
          @Override
          public Iterable<String> call(String s) {
            s = s.replace("\"", " ");
            s = s.replace(",", " ");
            s = s.toLowerCase();
            return Arrays.asList(SPACE.split(s));
          }
        });

    JavaPairRDD<String, Integer> ones = words
        .mapToPair(new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<String, Integer>(s, 1);
          }
        });

    JavaPairRDD<String, Integer> counts = ones
        .reduceByKey(new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });

    // reverse count and string
    JavaPairRDD<Integer, String> counts2 = counts
        .mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
          @Override
          public Tuple2<Integer, String> call(
              Tuple2<String, Integer> x) {
            return new Tuple2<Integer, String>(x._2(), x._1());
          }
        });

    counts2 = counts2.sortByKey(false, 1);
    return counts;
  }


}
