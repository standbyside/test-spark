package example;

import com.google.common.collect.Lists;
import java.util.Arrays;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import util.SparkUtils;


/**
 * 创建RDD.
 */
public class CreateRDDExamples {

  public static void main(String[] args) {
    createRDD();
  }

  public static void createRDD() {
    JavaSparkContext sc = SparkUtils.getJavaSparkContext();

    // 1.
    JavaRDD<String> rdd1 = sc.textFile("src/main/resources/input-files/english-words.txt");
    System.out.println("--------------1--------------");
    SparkUtils.printRDD(rdd1);

    // 2.
    JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("pandas", "i like pandas"));
    System.out.println("--------------2--------------");
    SparkUtils.printRDD(rdd2);

    // 3.
    JavaPairRDD<String, String> rdd3 = sc.parallelizePairs(Lists.newArrayList(
        new Tuple2<>("key1", "value1"),
        new Tuple2<>("key2", "value2"),
        new Tuple2<>("key3", "value3")
    ));
    System.out.println("--------------3--------------");
    SparkUtils.printPairRDD(rdd3);

    // 4.
    JavaPairRDD<String, String> rdd4 = rdd1.mapToPair(
        (PairFunction<String, String, String>) x -> new Tuple2(x.split(" ")[0], x)
    );
    System.out.println("--------------4--------------");
    SparkUtils.printPairRDD(rdd4);

  }
}
