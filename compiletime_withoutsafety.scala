import dataSetsEample.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
/////Explain runtime error
object compiletime_withoutsafety extends App{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.spark-project").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkConf =new SparkConf()
  sparkConf.set("spark.app.name","myfirstapplication")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  case class dataset1(order_id: Int,customer_id: Int)

  val ordersDf = spark.read
    .option("header",true)
    .option("inferSchema",true)
    .csv("D:/Big data/orders.csv")
    //.load
  //ordersDf.filter("order_id< 10").show //compile time not safety
  import spark.implicits._
  val ordersDs = ordersDf.as[dataset1]
  ordersDs.filter("order_id< 10").show
  scala.io.StdIn.readLine()
  spark.stop()
}
