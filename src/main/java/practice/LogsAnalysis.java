package practice;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.ExampleDataUtils;
import util.SparkUtils;

/**
 * 日志分析.
 */
public class LogsAnalysis {

  public static void main(String[] args) {
    JavaSparkContext sc = SparkUtils.getJavaSparkContext();
    // 读取输入文件
    JavaRDD<String> lines = sc.textFile(ExampleDataUtils.INPUT_FOLDER + "/logs-info.txt");
    // 切分为单词并且删掉空行
    JavaRDD<String[]> tokenized = lines
        .map(line -> line.split(" "))
        .filter(words -> words.length > 0);
    // 提取出每行的第一个单词(日志等级)并进行计数
    JavaPairRDD<String, Integer> counts = tokenized
        .mapToPair(o -> new Tuple2<>(o[0], 1))
        .reduceByKey((o1, o2) -> o1 + o2);
    // 查看RDD的谱系
    String s = counts.toDebugString();
    System.out.println(s);
    // 收集RDD
    counts.collect();
  }
}
