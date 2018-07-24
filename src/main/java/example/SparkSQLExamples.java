package example;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import util.ExampleDataUtils;
import util.SparkUtils;


/**
 * @author zhaona
 * @create 2018/8/7 下午2:19
 */
public class SparkSQLExamples {

  public static void main(String[] args) {
    write2();
  }

  public static void show() {
    Dataset<Row> dataset = ExampleDataUtils.getPersonDataset();
    System.out.println("--------------show--------------");
    dataset.show();
  }

  public static void printSchema() {
    Dataset<Row> dataset = ExampleDataUtils.getPersonDataset();
    System.out.println("--------------printSchema--------------");
    dataset.printSchema();
  }

  public static void select() {
    Dataset<Row> dataset = ExampleDataUtils.getPersonDataset();
    System.out.println("--------------select--------------");
    dataset.select(col("name").as("username"), col("age").plus(1)).show();
  }

  public static void filter() {
    Dataset<Row> dataset = ExampleDataUtils.getPersonDataset();
    System.out.println("--------------filter--------------");
    dataset.filter(col("age").gt(24)).show();
  }

  public static void groupBy() {
    Dataset<Row> dataset = ExampleDataUtils.getPersonDataset();
    System.out.println("--------------groupBy--------------");
    dataset.groupBy("age").count().show();
  }

  public static void sort() {
    Dataset<Row> dataset = ExampleDataUtils.getPersonDataset();
    System.out.println("--------------sort--------------");
    dataset.sort(col("age").asc(), col("name").desc()).show();
  }

  public static void tempView() {
    Dataset<Row> dataset = ExampleDataUtils.getPersonDataset();
    dataset.createOrReplaceTempView("person");
    // 仅当前session有效
    Dataset<Row> newset = SparkUtils.getSparkSession().sql("SELECT * FROM person");
    System.out.println("--------------tempView--------------");
    newset.show();
  }

  public static void globalTempView() {
    try {
      Dataset<Row> dataset = ExampleDataUtils.getPersonDataset();
      dataset.createGlobalTempView("person");
      System.out.println("--------------globalTempView--------------");
      // 全局临时视图与系统保留数据库global_temp绑定，是跨session的
      SparkUtils.getSparkSession().newSession().sql("SELECT * FROM global_temp.person").show();
    } catch (AnalysisException e) {
      e.printStackTrace();
    }
  }

  public static void write1() {
    Dataset<Row> dataset = ExampleDataUtils.getPersonDataset();
    // 支持json，parquet，jdbc，orc，libsvm，csv，text等格式文件
    dataset.write().format("json").save(ExampleDataUtils.OUTPUT_FOLDER + "/dataset-write.json");
    System.out.println("--------------write1--------------");
  }

  public static void write2() {
    Dataset<Row> dataset = ExampleDataUtils.getPersonDataset();
    dataset.rdd().saveAsTextFile(ExampleDataUtils.OUTPUT_FOLDER + "/dataset-write.json");
    System.out.println("--------------write2--------------");
  }
}
