package util;

import com.google.common.collect.Lists;
import entity.ApacheAccessLog;
import entity.Person;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

/**
 * @author zhaona
 * @create 2018/7/26 下午3:20
 */
public class ExampleDataUtils {

  public static final String INPUT_FOLDER = "src/main/resources/input-files";

  public static final String OUTPUT_FOLDER = "src/main/resources/output-files";

  public static JavaPairRDD<Integer, Integer> getIntegerJavaPairRDD1() {
    JavaPairRDD<Integer, Integer> rdd = SparkUtils.getJavaSparkContext()
        .parallelizePairs(
            Lists.newArrayList(
                new Tuple2<>(1, 2),
                new Tuple2<>(3, 4),
                new Tuple2<>(3, 6)
            )
        );
    return rdd;
  }

  public static JavaPairRDD<Integer, Integer> getIntegerJavaPairRDD2() {
    return SparkUtils.getJavaSparkContext()
        .parallelizePairs(
            Lists.newArrayList(
                new Tuple2<>(1, 2),
                new Tuple2<>(2, 3),
                new Tuple2<>(3, 4)
            )
        );
  }

  public static JavaPairRDD<Integer, Integer> getIntegerJavaPairRDD3() {
    return SparkUtils.getJavaSparkContext()
        .parallelizePairs(
            Lists.newArrayList(
                new Tuple2<>(1, 2),
                new Tuple2<>(3, 4),
                new Tuple2<>(5, 6)
            )
        );
  }

  public static JavaDoubleRDD getJavaDoubleRDD() {
    return SparkUtils.getJavaSparkContext()
        .parallelizeDoubles(
            Lists.newArrayList(1.0, 2.0, 0.5, 0.2, 5.5)
        );
  }

  public static JavaRDD<Person> getPersonJavaRDD() {
    return SparkUtils.getJavaSparkContext().parallelize(
        Arrays.asList(
            Person.builder().name("Jack").age(26).build(),
            Person.builder().name("John").age(24).build(),
            Person.builder().name("Jose").age(22).build()
        )
    );
  }

  public static Dataset<Row> getPersonDataset() {
    SparkSession session = SparkUtils.getSparkSession();
    Dataset<Row> dataset = session.read().json(ExampleDataUtils.INPUT_FOLDER + "/persons-json.txt");
    return dataset;
  }

  public static String[] loadCallSign() throws FileNotFoundException {
    Scanner scanner = new Scanner(new File(ExampleDataUtils.INPUT_FOLDER + "/call-sign.txt"));
    ArrayList<String> list = new ArrayList<>();
    while (scanner.hasNextLine()) {
      list.add(scanner.nextLine());
    }
    return list.toArray(new String[list.size()]);
  }

  public static JavaDStream<ApacheAccessLog> getAccessLogDStream() {
    JavaDStream<String> logData = SparkUtils.getJavaStreamingContext()
        .socketTextStream("localhost", 7777);
    // 转为log对象
    JavaDStream<ApacheAccessLog> accessLogsDStream = logData
        .map(o -> ApacheAccessLog.parseFromLogLine(o));
    return accessLogsDStream;
  }
}
