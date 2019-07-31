package example;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import util.SparkUtils;


public class SharedVariableExamples {

  private static JavaSparkContext sc = SparkUtils.getJavaSparkContext();

  public static void main(String[] args) {
    broadcast();
  }

  /**
   * 累加器.
   */
  public static void accumulator() {
    JavaRDD<String> rdd = sc.textFile("src/main/resources/input-files/english-words.txt");
    LongAccumulator accumulator = sc.sc().longAccumulator("long accumulator name");
    rdd.map(o -> {
      if (StringUtils.isBlank(o)) {
        accumulator.add(1);
      }
      return o;
    }).count();
    System.out.println("--------------accumulator--------------");
    System.out.println(accumulator.value());
  }

  /**
   * 广播变量
   */
  public static void broadcast() {
    Broadcast<String> broadcast = sc.broadcast("test broadcase");
    JavaRDD<String> rdd = sc.textFile("src/main/resources/input-files/english-words.txt");
    rdd.foreach(o -> System.out.println(o + "_" + broadcast.value()));
  }
}
