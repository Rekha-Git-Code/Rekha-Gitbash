import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object selectedColumn1 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
  //val Schema1 = "id Int,number Int"
  val df = spark.read
    .option("header", "false") // Since CSV file doesn't have a header
    //.csv("C:/Ranjini/test.csv")
    .option("path","C:/Ranjini/test.csv")
    .load
  //select desired column
  val selectedColumnsDf = df.selectExpr("_c0", "_c2")
  //rename column name
val columnNameDf=selectedColumnsDf.withColumnRenamed("_c0","id")
  .withColumnRenamed("_c2","number")
  // Show the resulting DataFrame
  selectedColumnsDf.show()
  columnNameDf.show()
  spark.stop()
}