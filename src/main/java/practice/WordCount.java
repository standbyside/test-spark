package practice;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.ExampleDataUtils;
import util.SparkUtils;

import java.util.Arrays;

/**
 * 单词计数器.
 */
public class WordCount {

  public static void main(String[] args) {

    JavaSparkContext sc = SparkUtils.getJavaSparkContext();

    JavaRDD<String> lines = sc.textFile(ExampleDataUtils.INPUT_FOLDER + "/english-words.txt");
    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

    JavaPairRDD<String, Integer> pairs = words
        .mapToPair(word -> new Tuple2<>(word, 1))
        .reduceByKey((o1, o2) -> o1 + o2);

    for (Tuple2<String, Integer> pair : pairs.collect()) {
      System.out.println(pair._1 + ": " + pair._2);
    }

    sc.close();
  }

}
