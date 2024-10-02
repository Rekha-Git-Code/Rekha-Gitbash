import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object testRddRead extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  //=================read 1st df==========================

val ratingSchema = "userid Int, movieid Int, rating Int, timestamp String"
  //1 :: 1193 :: 5 :: 978300760
  // 1 :: 661 :: 3 :: 978302109
  //1 :: 914 :: 3 :: 978301968
  val ratingsDf = spark.read
    .format("csv")
    .option("header", true)
    .schema(ratingSchema)
    .option("delimiter","::")
    .option("path","C:/Ranjini/Ranjini/Dataset/Dataset4/ratings.dat")
    .load()
  ratingsDf.show()
  ratingsDf.printSchema()
  //=================read 2nd df==========================
  val movieSchema="movieid Int, moviename String, genre String"
 val movieDf = spark.read
    .format("csv")
    .option("header", true)
    .schema(movieSchema)
    .option("delimiter", "::")
  .option("path","C:/Ranjini/Ranjini/Dataset/Dataset4/movies.dat")
   .load()
  movieDf.show()
  movieDf.printSchema()
  spark.stop()
}