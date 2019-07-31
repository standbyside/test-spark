package util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import org.apache.spark.streaming.Durations;

import java.util.Arrays;
import java.util.List;

public class SparkUtils {

  static {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
  }

  private static JavaSparkContext sc = new JavaSparkContext(
      new SparkConf().setMaster("local[*]").setAppName("My App")
  );

  private static SparkSession session = SparkSession.builder().config(sc.getConf())
      .enableHiveSupport().getOrCreate();

  private static JavaStreamingContext stc = new JavaStreamingContext(sc, Durations.seconds(10));

  static {
    stc.checkpoint(ExampleDataUtils.OUTPUT_FOLDER + "/check-points");
  }

  public static JavaSparkContext getJavaSparkContext() {
    return sc;
  }

  public static SparkSession getSparkSession() {
    return session;
  }

  public static JavaStreamingContext getJavaStreamingContext() {
    return stc;
  }

  /**
   * 打印RDD.
   */
  public static void printRDD(JavaRDD rdd) {
    List list = rdd.collect();
    for (int i=0;i<list.size();i++) {
      Object obj = list.get(i);
      if (obj.getClass().isArray()) {
        System.out.println(Arrays.toString((String[])obj));
      } else {
        System.out.println(list.get(i));
      }
    }
  }

  /**
   * 打印PairRDD.
   */
  public static void printPairRDD(JavaPairRDD rdd) {
    List<Tuple2<String, String>> list = rdd.collect();
    for (int i=0;i<list.size();i++) {
      System.out.println(list.get(i));
    }
  }
}
