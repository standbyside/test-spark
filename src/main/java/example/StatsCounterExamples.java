package example;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.util.StatCounter;
import util.ExampleDataUtils;

/**
 * 数值RDD的操作.
 *
 * Spark对包含数值数据的RDD提供了一些描述性的统计操作。
 * Spark的数值操作是通过流式算法实现的，允许以每次一个元素的方式构建出模型。
 * 这些统计数据都会在调用stats()时通过一次遍历数据计算出来，并以StatsCounter对象返回。
 *
 * @author zhaona
 * @create 2018/7/30 下午3:58
 */
public class StatsCounterExamples {

  public static void main(String[] args) {
    JavaDoubleRDD rdd = ExampleDataUtils.getJavaDoubleRDD();
    StatCounter stats = rdd.stats();
    // 元素个数.
    System.out.println("--------------count--------------");
    System.out.println(stats.count());
    // 平均值.
    System.out.println("--------------mean--------------");
    System.out.println(stats.mean());
    // 总和.
    System.out.println("--------------sum--------------");
    System.out.println(stats.sum());
    // 最大值.
    System.out.println("--------------max--------------");
    System.out.println(stats.max());
    // 最小值.
    System.out.println("--------------min--------------");
    System.out.println(stats.min());
    // 方差.
    System.out.println("--------------variance--------------");
    System.out.println(stats.variance());
    // 从采样中计算的方差.
    System.out.println("--------------sampleVariance--------------");
    System.out.println(stats.sampleVariance());
    // 标准差.
    System.out.println("--------------stdev--------------");
    System.out.println(stats.stdev());
    // 从采样中计算的标准差.
    System.out.println("--------------sampleStdev--------------");
    System.out.println(stats.sampleStdev());
  }
}
