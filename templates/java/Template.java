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

public final class $toolName implements Serializable {
  private static final long serialVersionUID = -2312231231L;
  
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: $toolName <input> <output>");
      System.exit(1);
    }
    
    String input_dir = args[0];
    String output_dir = args[1];

    SparkConf sparkConf = new SparkConf().setAppName("$toolName" + input_dir + " " + output_dir);
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    JavaRDD<String> input = sc.textFile(input_dir, 1);

    $toolName instance = new $toolName();
    // adapt ouput type if changed
    JavaRDD<String> result = instance.doJob(input);
    result.saveAsTextFile(output_dir);

    sc.stop();
  }
  
  /* adapt input and ouptut type as needed */
  public JavaRDD<String> doJob(JavaRDD<String> input) {
    return input;
  }


}
