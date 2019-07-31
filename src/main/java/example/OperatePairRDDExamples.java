package example;

import com.google.common.collect.Lists;
import entity.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple3;
import util.ExampleDataUtils;
import util.SparkUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * 键值对操作.
 */
public class OperatePairRDDExamples {

  public static void main(String[] args) {
    flatMapValues();
  }

  /**
   * ----聚合操作--------------------------------------------------------
   */

  public static void keys() {
    JavaPairRDD<Integer, Integer> rdd1 = ExampleDataUtils.getIntegerJavaPairRDD1();
    JavaRDD<Integer> rdd2 = rdd1.keys();
    System.out.println("--------------keys--------------");
    SparkUtils.printRDD(rdd2);
  }

  public static void values() {
    JavaPairRDD<Integer, Integer> rdd1 = ExampleDataUtils.getIntegerJavaPairRDD1();
    JavaRDD<Integer> rdd2 = rdd1.values();
    System.out.println("--------------values--------------");
    SparkUtils.printRDD(rdd2);
  }

  public static void mapValues() {
    JavaPairRDD<Integer, Integer> rdd = ExampleDataUtils.getIntegerJavaPairRDD1();
    rdd = rdd.mapValues(o -> o * 2);
    System.out.println("--------------mapValues--------------");
    SparkUtils.printPairRDD(rdd);
  }

  /**
   * flatMapValues，对Pair RDD中的每个值应用一个返回迭代器的函数
   */
  public static void flatMapValues() {
    JavaPairRDD<Integer, Integer> rdd = ExampleDataUtils.getIntegerJavaPairRDD1();
    rdd = rdd.flatMapValues((Function<Integer, Iterable<Integer>>) v1 ->
        Lists.newArrayList(v1, v1 + 1, v1 * 2));
    System.out.println("--------------flatMapValues--------------");
    SparkUtils.printPairRDD(rdd);
  }

  public static void reduceByKey() {
    JavaPairRDD<Integer, Integer> rdd = ExampleDataUtils.getIntegerJavaPairRDD1();
    rdd = rdd.reduceByKey((o1, o2) -> o1 + o2);
    System.out.println("--------------reduceByKey--------------");
    SparkUtils.printPairRDD(rdd);
  }

  public static void foldByKey() {
    JavaPairRDD<Integer, Integer> rdd = ExampleDataUtils.getIntegerJavaPairRDD1();
    rdd = rdd.foldByKey(10, (o1, o2) -> o1 + o2);
    System.out.println("--------------foldByKey--------------");
    SparkUtils.printPairRDD(rdd);
  }

  /**
   * 自定义合并的行为.
   */
  public static void combineByKey() {
    Function<Integer, AvgCount> createAcc = (Function<Integer, AvgCount>) x -> new AvgCount(x, 1);

    Function2<AvgCount, Integer, AvgCount> addAndCount =
        (Function2<AvgCount, Integer, AvgCount>) (a, x) -> {
          a.total += x;
          a.num += 1;
          return a;
        };

    Function2<AvgCount, AvgCount, AvgCount> combine =
        (Function2<AvgCount, AvgCount, AvgCount>) (a, b) -> {
          a.total += b.total;
          a.num += b.num;
          return a;
        };

    JavaPairRDD<Integer, Integer> rdd = ExampleDataUtils.getIntegerJavaPairRDD1();
    JavaPairRDD<Integer, AvgCount> avgCounts = rdd.combineByKey(createAcc, addAndCount, combine);
    Map<Integer, AvgCount> map = avgCounts.collectAsMap();
    System.out.println("--------------combineByKey--------------");
    System.out.println(map);
  }

  /**
   * ----数据分组--------------------------------------------------------
   */

  public static void groupByKey() {
    JavaPairRDD<Integer, Integer> rdd1 = ExampleDataUtils.getIntegerJavaPairRDD1();
    JavaPairRDD<Integer, Iterable<Integer>> rdd2 = rdd1.groupByKey();
    System.out.println("--------------groupByKey--------------");
    SparkUtils.printPairRDD(rdd2);
  }

  /**
   * groupBy接收一个函数，这个函数返回的值作为key，然后通过这个key来对里面的元素进行分组.
   */
  public static void groupBy() {
    Function<Tuple2<Integer, Integer>, Integer> func =
        (Function<Tuple2<Integer, Integer>, Integer>) o -> o._2 - 2;
    JavaPairRDD<Integer, Integer> rdd1 = ExampleDataUtils.getIntegerJavaPairRDD1();
    JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> rdd2 = rdd1.groupBy(func);
    System.out.println("--------------groupBy--------------");
    SparkUtils.printPairRDD(rdd2);
  }

  /**
   * ----数据连接--------------------------------------------------------
   */

  public static void join() {
    JavaPairRDD<Integer, Integer> rdd1 = ExampleDataUtils.getIntegerJavaPairRDD2();
    JavaPairRDD<Integer, Integer> rdd2 = ExampleDataUtils.getIntegerJavaPairRDD3();
    JavaPairRDD<Integer, Tuple2<Integer, Integer>> rdd3 = rdd1.join(rdd2);
    System.out.println("--------------join--------------");
    SparkUtils.printPairRDD(rdd3);
  }

  public static void fullOuterJoin() {
    JavaPairRDD<Integer, Integer> rdd1 = ExampleDataUtils.getIntegerJavaPairRDD2();
    JavaPairRDD<Integer, Integer> rdd2 = ExampleDataUtils.getIntegerJavaPairRDD3();
    JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<Integer>>> rdd3 = rdd1.fullOuterJoin(rdd2);
    System.out.println("--------------fullOuterJoin--------------");
    SparkUtils.printPairRDD(rdd3);
  }

  public static void leftOuterJoin() {
    JavaPairRDD<Integer, Integer> rdd1 = ExampleDataUtils.getIntegerJavaPairRDD2();
    JavaPairRDD<Integer, Integer> rdd2 = ExampleDataUtils.getIntegerJavaPairRDD3();
    JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> rdd3 = rdd1.leftOuterJoin(rdd2);
    System.out.println("--------------leftOutJoinRDD--------------");
    SparkUtils.printPairRDD(rdd3);
  }

  public static void rightOuterJoin() {
    JavaPairRDD<Integer, Integer> rdd1 = ExampleDataUtils.getIntegerJavaPairRDD2();
    JavaPairRDD<Integer, Integer> rdd2 = ExampleDataUtils.getIntegerJavaPairRDD3();
    JavaPairRDD<Integer, Tuple2<Optional<Integer>, Integer>> rdd3 = rdd1.rightOuterJoin(rdd2);
    System.out.println("--------------leftOutJoinRDD--------------");
    SparkUtils.printPairRDD(rdd3);
  }

  /**
   * cogroup，将多个RDD中拥有相同键的数据分组到一起.
   */
  public static void cogroup() {
    JavaPairRDD<Integer, Integer> rdd1 = ExampleDataUtils.getIntegerJavaPairRDD1();
    JavaPairRDD<Integer, Integer> rdd2 = ExampleDataUtils.getIntegerJavaPairRDD2();
    JavaPairRDD<Integer, Integer> rdd3 = ExampleDataUtils.getIntegerJavaPairRDD3();
    JavaPairRDD<Integer, Tuple3<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>> rdd
        = rdd1.cogroup(rdd2, rdd3);
    System.out.println("--------------cogroup--------------");
    SparkUtils.printPairRDD(rdd);
  }

  /**
   * subtractByKey，删掉rdd1中键与rdd2中的键相同的元素.
   */
  public static void subtractByKey() {
    JavaPairRDD<Integer, Integer> rdd1 = ExampleDataUtils.getIntegerJavaPairRDD2();
    JavaPairRDD<Integer, Integer> rdd2 = ExampleDataUtils.getIntegerJavaPairRDD3();
    JavaPairRDD<Integer, Integer> rdd3 = rdd1.subtractByKey(rdd2);
    System.out.println("--------------subtractByKey--------------");
    SparkUtils.printPairRDD(rdd3);
  }

  /**
   * ----数据排序--------------------------------------------------------
   */

  public static void sortByKey() {
    JavaPairRDD<Integer, Integer> rdd = ExampleDataUtils.getIntegerJavaPairRDD1();
    // 正序
    rdd = rdd.sortByKey();
    // 倒序
    rdd = rdd.sortByKey(false);
    // 指定Comparator
    rdd = rdd.sortByKey(Comparator.comparingInt(o -> o));
    System.out.println("--------------sortByKey--------------");
    SparkUtils.printPairRDD(rdd);
  }

  /**
   * ----行动操作--------------------------------------------------------
   */

  public static void countByKey() {
    JavaPairRDD<Integer, Integer> rdd = ExampleDataUtils.getIntegerJavaPairRDD1();
    Map<Integer, Long> map = rdd.countByKey();
    System.out.println("--------------countByKey--------------");
    System.out.println(map);
  }

  public static void collectAsMap() {
    JavaPairRDD<Integer, Integer> rdd = ExampleDataUtils.getIntegerJavaPairRDD1();
    Map<Integer, Integer> map = rdd.collectAsMap();
    System.out.println("--------------collectAsMap--------------");
    System.out.println(map);
  }

  /**
   * 返回给定键对应的所有值.
   */
  public static void lookup() {
    JavaPairRDD<Integer, Integer> rdd = ExampleDataUtils.getIntegerJavaPairRDD1();
    List<Integer> list = rdd.lookup(4);
    System.out.println("--------------lookup--------------");
    System.out.println(list);
  }
}
