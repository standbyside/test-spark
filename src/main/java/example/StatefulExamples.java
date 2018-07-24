package example;

import entity.ApacheAccessLog;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import util.ExampleDataUtils;

/**
 * Spark Streaming 有状态转化操作.
 *
 * @author zhaona
 * @create 2018/8/9 下午4:35
 */
public class StatefulExamples {

  public static void window() {
    JavaDStream<ApacheAccessLog> accessLogsDStream = ExampleDataUtils.getAccessLogDStream();

    JavaDStream<ApacheAccessLog> accessLogsWindow = accessLogsDStream
        .window(
            Durations.seconds(30), Durations.seconds(10)
        );

    JavaDStream<Long> windowCounts = accessLogsWindow.count();
    windowCounts.print();
  }

  public static void reduceByWindow() {
    // TODO
  }

  public static void reduceByKeyAndWindow() {
    // TODO
  }

}
