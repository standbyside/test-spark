package example;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.SparkUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 常见的转化操作和行动操作.
 */
public class OperateRDDExamples {

  private static JavaSparkContext sc = SparkUtils.getJavaSparkContext();

  public static void main(String[] args) {
    aggregate();
  }

  /**
   * ----转化操作--------------------------------------------------------
   */

  public static void map() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3));
    rdd = rdd.map(o -> o * o);
    System.out.println("--------------map--------------");
    SparkUtils.printRDD(rdd);
  }

  public static void filter() {
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("string1", "string2", "string3"));
    rdd = rdd.filter(o -> o.contains("3"));
    System.out.println("--------------filter--------------");
    SparkUtils.printRDD(rdd);
  }

  public static void flatMap() {
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello world", "hi"));
    rdd = rdd.flatMap(o -> Arrays.asList(o.split(" ")).iterator());
    System.out.println("--------------flatMap--------------");
    SparkUtils.printRDD(rdd);
  }

  /**
   * ----伪集合操作--------------------------------------------------------
   */

  /**
   * distinct，去重.
   * 操作的开销很大，因为需要将数据通过网络混洗(shuffle)，以确保每个元素都只有一份.
   */
  public static void distinct() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    rdd = rdd.distinct();
    System.out.println("--------------distinct--------------");
    SparkUtils.printRDD(rdd);
  }

  /**
   * union，合并（含重复元素）.
   */
  public static void union() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
    JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(2, 3, 4));
    JavaRDD<Integer> rdd3 = rdd1.union(rdd2);
    System.out.println("--------------union--------------");
    SparkUtils.printRDD(rdd3);
  }

  /**
   * intersection，取交集.
   * intersection与union的概念相似，intersection的性能却要差很多，因为需要网络混洗发现共有元素.
   */
  public static void intersection() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
    JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(2, 3, 4));
    JavaRDD<Integer> rdd3 = rdd1.intersection(rdd2);
    System.out.println("--------------intersection--------------");
    SparkUtils.printRDD(rdd3);
  }

  /**
   * subtract，返回一个由存在于rdd1而不存在于rdd2的元素组成的RDD.
   * 和intersection()一样，也需要数据混洗.
   */
  public static void subtract() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
    JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(2, 3, 4));
    JavaRDD<Integer> rdd3 = rdd1.subtract(rdd2);
    System.out.println("--------------subtract--------------");
    SparkUtils.printRDD(rdd3);
  }

  /**
   * cartesian，笛卡尔积.
   * 操作会返回所有可能的 (a, b) 对，a来自rdd1，b来自rdd2.
   */
  public static void cartesian() {
    JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3));
    JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(2, 3, 4));
    JavaPairRDD<Integer, Integer> rdd3 = rdd1.cartesian(rdd2);
    System.out.println("--------------cartesian--------------");
    SparkUtils.printPairRDD(rdd3);
  }

  /**
   * ----行动操作--------------------------------------------------------
   */

  /**
   * 返回RDD中所有元素.
   */
  public static void collect() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    List<Integer> list = rdd.collect();
    System.out.println("--------------collect--------------");
    System.out.println(list);
  }

  /**
   * 返回RDD中的元素个数.
   */
  public static void count() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    long num = rdd.count();
    System.out.println("--------------count--------------");
    System.out.println(num);
  }

  /**
   * 各元素在RDD中出现的次数.
   */
  public static void countByValue() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    Map<Integer, Long> map = rdd.countByValue();
    System.out.println("--------------countByValue--------------");
    System.out.println(map);
  }

  /**
   * 从RDD中返回n个元素.
   */
  public static void take() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    List<Integer> list = rdd.take(2);
    System.out.println("--------------take--------------");
    System.out.println(list);
  }

  /**
   * 从RDD中返回最前面的n个元素.
   */
  public static void top() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    List<Integer> list = rdd.top(4);
    System.out.println("--------------top--------------");
    System.out.println(list);
  }

  /**
   * 从RDD中按照提供的顺序返回最前面的n个元素.
   */
  public static void takeOrdered() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    List<Integer> list = rdd.takeOrdered(4, Comparator.comparing(o -> o));
    System.out.println("--------------takeOrdered--------------");
    System.out.println(list);
  }

  /**
   * 从RDD中会返回任意一些元素.
   */
  public static void takeSample() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    List<Integer> list = rdd.takeSample(false, 4);
    System.out.println("--------------takeSample--------------");
    System.out.println(list);
  }

  /**
   * 并行整合RDD中所有数据.
   */
  public static void reduce() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    Integer sum = rdd.reduce((x, y) -> x + y);
    System.out.println("--------------reduce--------------");
    System.out.println(sum);
  }

  /**
   * 和reduce()一样，但是需要提供初始值.
   */
  public static void fold() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    Integer sum = rdd.fold(4, (x, y) -> x + y);
    System.out.println("--------------fold--------------");
    System.out.println(sum);
  }

  /**
   * 和reduce()相似，但是通常返回不同的类型.
   */
  public static void aggregate() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    Tuple2<Double, Integer> t = rdd.aggregate(
        new Tuple2<>(0.0, 0),
        (x, y) -> new Tuple2<>(x._1 + y, x._2 + 1),
        (x, y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2)
    );
    System.out.println("--------------aggregate--------------");
    System.out.println(t._1/t._2);
  }

  /**
   * 对RDD中的每个元素使用给定的函数.
   */
  public static void foreach() {
    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 2, 1));
    System.out.println("--------------foreach--------------");
    rdd.foreach(o -> System.out.println(o));
  }
}
