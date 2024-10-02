//import assign2.Ratings
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object wc3 extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.spark-project").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
  case class wordCount(words:String)
  val inputRDD = spark.sparkContext.textFile("D:/Big data/Spark/data.txt")
  inputRDD.collect.foreach(println)
  val wordsRDD1 = inputRDD.flatMap(x => x.split(" "))
  wordsRDD1.collect.foreach(println)
  val wordsRDD2=wordsRDD1.map(x => wordCount(x))
  wordsRDD2.collect.foreach(println)
  import spark.implicits._
  val countsDf = wordsRDD2.toDF()
  countsDf.groupBy("words").count().show
  spark.stop()
}
