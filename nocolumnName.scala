//import assign2.Ratings
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object nocolumnName extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
  case class wordCount(id:Int,name:String)
  val inputRDD = spark.sparkContext.textFile("C:/Ranjini/test.csv")
  //inputRDD.collect.foreach(println)
  import spark.implicits._
  val customerDf = inputRDD.toDF()
  customerDf.show()
  spark.stop()
}


