package nl.utwente.bigdata;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import scala.Tuple2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

public class TestUtils {
  private static JavaSparkContext context = null;
  public static JavaSparkContext getSparkContext() {
    if (context == null) {
      SparkConf conf = new SparkConf().setAppName("Testing").setMaster("local[1]");
      context = new JavaSparkContext(conf);
    }
    return context;
  }
  
  public static class TupleComparator<X extends Comparable<? super X>,Y extends Comparable<? super Y>> implements Comparator<Tuple2<X,Y>> {
    public int compare(Tuple2<X,Y> x, Tuple2<X,Y> y) {
      int cmp = x._1.compareTo(y._1);
      if (cmp != 0) return cmp;
      return x._2.compareTo(y._2);
    }
  } 
  
  public static <X> boolean listEquals(List<X> l1, List<X> l2, Comparator<X> comparator) {
    if (l1.size() != l2.size()) return false;
    ArrayList<X> ll1 = new ArrayList<X>(l1);
    ArrayList<X> ll2 = new ArrayList<X>(l2);
    Collections.sort(ll1, comparator);
    Collections.sort(ll2, comparator);
    Iterator<X> it1 = ll1.iterator();
    Iterator<X> it2 = ll2.iterator();
    
    while (it1.hasNext() && it2.hasNext()) {
      X x = it1.next();
      X y = it2.next();
      if (comparator.compare(x,y) != 0) return false;
    }
    return true;
  }
}