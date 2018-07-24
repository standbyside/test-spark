package example;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import util.ExampleDataUtils;
import util.SparkUtils;

/**
 * @author zhaona
 * @create 2018/8/9 上午10:22
 */
public class SparkStreamingExamples {

  public static void main(String[] args) {
    try {
      JavaStreamingContext stc = SparkUtils.getJavaStreamingContext();
      // 监听流
      JavaDStream<String> lines = socketTextStream(stc);
      // 对片段RDD进行操作
      JavaDStream<String> words = lines.flatMap(
          (FlatMapFunction<String, String>) s -> Lists.newArrayList(s.split(" ")).iterator()
      );
      words.print();
      // 启动流计算环境StreamingContext并等待它"完成"
      stc.start();
      // 等待作业完成
      stc.awaitTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static JavaDStream<String> socketTextStream(JavaStreamingContext stc) {
    // 以端口7777作为输入来源创建DStream
    return stc.socketTextStream("localhost", 7777);
  }

  public static JavaDStream<String> textFileStream(JavaStreamingContext stc) {
    // 监控文件夹
    return stc.textFileStream(ExampleDataUtils.INPUT_FOLDER + "/log-files");
  }

}
