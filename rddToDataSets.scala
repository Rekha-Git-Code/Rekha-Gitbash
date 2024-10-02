import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object rddToDataSets extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
  case class wordCount(words:String)
  val inputRDD = spark.sparkContext.textFile("C:/Ranjini/Ranjini/Dataset/data.txt")
  //inputRDD.collect.foreach(println)
  val wordsRDD1 = inputRDD.flatMap(x => x.split(" "))
  //wordsRDD1.collect.foreach(println)
  val wordsRDD2=wordsRDD1.map(x => wordCount(x))
  //wordsRDD2.collect.foreach(println)
  //case class wordCount1(words:String)
  import spark.implicits._
  val countsDs = wordsRDD2.toDS()
 countsDs.groupBy(col("words")).count.show()
 /// countsDs.filter("words == 'hi' ")
  spark.stop()
}