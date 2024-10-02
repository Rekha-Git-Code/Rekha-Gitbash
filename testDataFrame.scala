import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger

object testDataFrame extends App{
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
  val ordersDf = spark.read  //1st action read the csv
    .option("header",true)
    .option("inferSchema",true)//2nd action infer the schema//not used in production//manually specify the schema
    .csv("D:/Big data/Spark/orders.csv")
  //driver convert higher level code to rdd and send to executors
  val groupedOrdersDf = ordersDf
    .repartition(4)      //transformation
    .where("order_customer_id>8000") //wide transformation
    .select("order_id","order_customer_id") //transformation
    .groupBy("order_customer_id") //wide transformation
    .count() //transformation
  groupedOrdersDf.foreach(x=>{
    println(x)  //Executors
  })

  scala.io.StdIn.readLine()
  spark.stop()
}
