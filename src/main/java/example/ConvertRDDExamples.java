package example;

import com.google.common.collect.Lists;
import entity.Person;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import util.ExampleDataUtils;
import util.SparkUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * RDD互相转换.
 *
 * @author zhaona
 * @create 2018/7/26 下午2:56
 */
public class ConvertRDDExamples {

  private static JavaSparkContext sc = SparkUtils.getJavaSparkContext();

  public static void main(String[] args) {
    rddToPairRdd();
    pairRddToRdd();
  }

  public static void rddToPairRdd() {
    JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a as", "b bs"));
    JavaPairRDD<String, String> rdd2 = rdd1.mapToPair(
        o -> new Tuple2<>(o.split(" ")[0], o.split(" ")[1])
    );
    System.out.println("--------------rddToPairRdd--------------");
    SparkUtils.printPairRDD(rdd2);
  }

  public static void pairRddToRdd() {
    JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Lists.newArrayList(
        new Tuple2<>("key1", "value1"),
        new Tuple2<>("key2", "value2"),
        new Tuple2<>("key3", "value3")
    ));
    JavaRDD<String> rdd2 = rdd1.map(o -> o._1 + "_" + o._2);
    System.out.println("--------------pairRddToRdd--------------");
    SparkUtils.printRDD(rdd2);
  }

  /**
   * 利用反射机制推断RDD模式.
   */
  public static void rddToDatasetByReflection() {
    JavaRDD<Person> rdd = ExampleDataUtils.getPersonJavaRDD();
    Dataset<Row> dataset = SparkUtils.getSparkSession().createDataFrame(rdd, Person.class);
    System.out.println("--------------rddToDatasetByReflection--------------");
    dataset.show();
  }

  /**
   * 使用编程方式定义RDD模式.
   */
  public static void rddToDatasetByPrograming() {
    JavaRDD<Person> rdd = ExampleDataUtils.getPersonJavaRDD();
    // 使用string定义schema
    String schemaString = "name,age";
    // 基于用字符串定义的schema生成StructType
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : schemaString.split(",")) {
      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
      fields.add(field);
    }
    StructType schema = DataTypes.createStructType(fields);

    // 把RDD转换为RowRDD
    JavaRDD<Row> rowRDD = rdd.map(record -> RowFactory.create(record.getName(), record.getAge()+""));
    // 对RowRDD应用schema
    Dataset<Row> dataset = SparkUtils.getSparkSession().createDataFrame(rowRDD, schema);
    System.out.println("--------------rddToDatasetByPrograming--------------");
    dataset.show();
  }
}
