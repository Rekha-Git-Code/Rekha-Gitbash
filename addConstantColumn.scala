import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

object addConstantColumn extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.spark-project").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplicadtion")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
  val Schema1 = "Id Int,User_name string"
  val df1 = spark.read
    .format("csv")
    .option("header", "true")
    .schema(Schema1)
    // false,Since CSV file doesn't have a header
    .option("path","D:/Big data/Big data project/Dataset/New folder/CSV files/user.csv")
    .load
  // Adding a Constant Column to DataFrame
  df1.show()
  val df2 = df1.select(col("Id"),col("User_name"),lit("Active").as("status"))
  df2.show()
  spark.stop()
}