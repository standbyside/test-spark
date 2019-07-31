package example;

import org.apache.spark.api.java.JavaSparkContext;
import util.SparkUtils;

/**
 * 基于分区的操作.
 *
 * 基于分区对数据进行操作可以让我们避免为每个数据元素进行重复的配置工作。诸如打开数据库连接或创建随机数生成器等操作。
 * Spark提供基于分区的map和foreach，让部分代码只对RDD的每个分区运行一次，降低这些操作的代价。
 */
public class PartitionsExamples {

  private static JavaSparkContext sc = SparkUtils.getJavaSparkContext();

  public static void main(String[] args) {

  }

  public static void mapPartitions() {

  }

  public static void mapPartitionsWithIndex() {

  }

  public static void foreachPartitions() {

  }


}
