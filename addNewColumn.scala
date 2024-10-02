import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

object addNewColumn extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
  //val Schema1 = "id Int,number Int"
  val df1 = spark.read
    .option("header", "true") // false,Since CSV file doesn't have a header
    .option("path","C:/Ranjini/test.csv")
    .load
  // Adding a Constant Column to DataFrame
  df1.show()
 //val df2 = df1.select(col("empid"),col("salary"),lit("Active").as("status"))
  //df2.show()
  spark.stop()
}
