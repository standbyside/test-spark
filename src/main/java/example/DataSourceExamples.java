package example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import entity.Person;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import util.ExampleDataUtils;
import util.SparkUtils;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

/**
 * 数据读取与保存.
 *
 * @author zhaona
 * @create 2018/7/26 下午3:55
 */
public class DataSourceExamples {

  private static JavaSparkContext sc = SparkUtils.getJavaSparkContext();

  public static void main(String[] args) {
    writeObject();
  }

  /**
   * ----文本文件--------------------------------------------------------
   */

  /**
   * 读取单个文本文件.
   */
  public static void readTextFile1() {
    JavaRDD<String> rdd1 = sc.textFile(ExampleDataUtils.INPUT_FOLDER + "/english-words.txt");
    System.out.println("--------------read text file 1--------------");
    SparkUtils.printRDD(rdd1);
  }

  /**
   * 读取整个文件夹.
   */
  public static void readTextFile2() {
    JavaRDD<String> rdd2 = sc.textFile(ExampleDataUtils.INPUT_FOLDER);
    System.out.println("--------------read text file 2--------------");
    SparkUtils.printRDD(rdd2);
  }

  /**
   * 读取整个文件夹，文件名作为key.
   */
  public static void readTextFile3() {
    JavaPairRDD<String, String> rdd3 = sc.wholeTextFiles(ExampleDataUtils.INPUT_FOLDER);
    System.out.println("--------------read text file 3--------------");
    SparkUtils.printPairRDD(rdd3);
  }

  /**
   * 保存文本文件.
   */
  public static void writeTextFile() {
    JavaRDD<String> rdd = sc.parallelize(Arrays.asList("test1", "test2", "test3"));
    rdd.saveAsTextFile(ExampleDataUtils.OUTPUT_FOLDER + "/spark-save-file-test/" + UUID.randomUUID().toString());
    System.out.println("--------------write text file--------------");
  }

  /**
   * ----JSON--------------------------------------------------------
   */

  /**
   * 读取json文件.
   */
  public static void readJson() {
    JavaRDD<String> input = sc.textFile(ExampleDataUtils.INPUT_FOLDER + "/persons-json.txt");
    JavaRDD<Person> personsRDD = input.mapPartitions(
        (FlatMapFunction<Iterator<String>, Person>) lines -> {
          ArrayList<Person> persons = new ArrayList<>();
          ObjectMapper mapper = new ObjectMapper();
          while (lines.hasNext()) {
            persons.add(mapper.readValue(lines.next(), Person.class));
          }
          return persons.iterator();
        }
    );
    System.out.println("--------------read json--------------");
    SparkUtils.printRDD(personsRDD);
  }

  /**
   * 保存json文件.
   */
  public static void writeJson() {
    JavaRDD<Person> rdd = ExampleDataUtils.getPersonJavaRDD();
    JavaRDD<String> output = rdd.mapPartitions(
        (FlatMapFunction<Iterator<Person>, String>) persons -> {
          ArrayList<String> lines = new ArrayList<>();
          ObjectMapper mapper = new ObjectMapper();
          while (persons.hasNext()) {
            lines.add(mapper.writeValueAsString(persons.next()));
          }
          return lines.iterator();
        }
    );
    output.saveAsTextFile(ExampleDataUtils.OUTPUT_FOLDER + "/spark-save-json-test/" + UUID.randomUUID().toString());
    System.out.println("--------------write json--------------");
  }

  /**
   * ----CSV--------------------------------------------------------
   */

  /**
   * 读取csv文件（CSV的所有数据字段均没有包含换行符）.
   */
  public static void readCSV1() {
    JavaRDD<String> input1 = sc.textFile(ExampleDataUtils.INPUT_FOLDER + "/persons-info.csv");
    JavaRDD<String[]> csv1 = input1.map((Function<String, String[]>) line ->
        new CSVReader(new StringReader(line)).readNext());
    System.out.println("--------------read csv 1--------------");
    SparkUtils.printRDD(csv1);
  }

  /**
   * 读取csv文件（完整读入每个文件，然后解析各段）.
   * 如果每个文件都很大，读取和解析的过程可能会成为性能瓶颈.
   */
  public static void readCSV2() {
    JavaPairRDD<String, String> input2 = sc.wholeTextFiles(ExampleDataUtils.INPUT_FOLDER + "/persons-csv");
    JavaRDD<String[]> csv2 = input2.flatMap((FlatMapFunction<Tuple2<String, String>, String[]>) file ->
        new CSVReader(new StringReader(file._2())).readAll().iterator()
    );
    System.out.println("--------------read csv 2--------------");
    SparkUtils.printRDD(csv2);
  }

  /**
   * 保存csv文件.
   */
  public static void writeCSV() {
    JavaRDD<Person> rdd = ExampleDataUtils.getPersonJavaRDD();
    rdd.map(o -> {
      StringWriter sw = new StringWriter();
      CSVWriter cw = new CSVWriter(sw);
      cw.writeNext(new String[] {o.getName(), String.valueOf(o.getAge())});
      cw.close();
      return sw.toString();
    })
    .saveAsTextFile(ExampleDataUtils.OUTPUT_FOLDER + "/spark-save-csv-test/" + UUID.randomUUID().toString());
    System.out.println("--------------write csv--------------");
  }

  /**
   * ----SequenceFile--------------------------------------------------------
   */

  /**
   * ----对象文件--------------------------------------------------------
   */

  public static void writeObject() {
    JavaRDD<Person> rdd = ExampleDataUtils.getPersonJavaRDD();
    rdd.saveAsObjectFile(ExampleDataUtils.OUTPUT_FOLDER + "/spark-save-object-test/" + UUID.randomUUID().toString());
    System.out.println("--------------write object--------------");
  }

}
