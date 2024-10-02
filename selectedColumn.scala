
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import rddToDataFrame.{inputRDD, wordCount, wordsRDD1}

object selectedColumn extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()


  case class Customer(id: Int, number: Int)

  val inputRDD = spark.sparkContext.textFile("C:/Ranjini/test.csv")

  val inputRDD1 = inputRDD.map(x => x.split(","))
  val inputRDD2 = inputRDD1.map(x => Customer(x(0).toInt, x(2).toInt))

  import spark.implicits._

  val customerDf = inputRDD2.toDF()
  customerDf.show()
  spark.stop()
}
