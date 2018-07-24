package example;

import entity.ApacheAccessLog;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import util.ExampleDataUtils;

/**
 * Spark Streaming 无状态转化操作.
 *
 * @author zhaona
 * @create 2018/8/9 下午4:35
 */
public class StatelessExamples {

  public static void operation() {

    JavaDStream<ApacheAccessLog> accessLogsDStream = ExampleDataUtils.getAccessLogDStream();

    // 提取每次访问的IP地址
    JavaPairDStream<String, Long> ipDStream = accessLogsDStream
        .mapToPair(new Functions.IpTuple());
    // 计算各IP访问次数
    JavaPairDStream<String, Long> ipCountsDStream = ipDStream
        .reduceByKey(new Functions.LongSumReducer());

    // 提取每次访问的传输数据
    JavaPairDStream<String, Long> ipBytesDStream = accessLogsDStream
        .mapToPair(new Functions.IpContentTuple());
    // 计算各IP传输数据量
    JavaPairDStream<String, Long> ipBytesSumDStream = ipBytesDStream
        .reduceByKey(new Functions.LongSumReducer());

    // 以IP地址为键，把请求计数的数据和传输数据量的数据连接起来
    JavaPairDStream<String, Tuple2<Long, Long>> ipBytesRequestCountDStream =
        ipCountsDStream.join(ipBytesSumDStream);
  }

  public static void transform() {
    JavaDStream<ApacheAccessLog> accessLogsDStream = ExampleDataUtils.getAccessLogDStream();
    // TODO
  }


}
