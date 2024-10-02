import org.apache.spark.sql.SparkSession
import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import java.sql.Timestamp
import java.sql.Time


//case class OrderData(order_id:Int,order_date:Time,order_customer_id:Int,order_status:String)
object withoutSafety extends App{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.spark-project").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val sparkConf =new SparkConf()
  sparkConf.set("spark.appname","myfirstapplication")
  sparkConf.set("spark.master","local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  case class OrderData(order_id:Int,order_date :Timestamp,order_item:String,order_total:Double,cust_id:Int)

  val Schema1= "order_id Int,order_date Timestamp,order_item String,order_total Double,cust_id Int"
  val ordersDf:Dataset[Row] = spark.read
    .option("header",true)
    //.option("inferSchema",true)
    .format("csv")
    .option("path","C:/Users/USER/Desktop/order1.csv")
    .schema(Schema1)
    //.csv("C:/Users/USER/Desktop/order1.csv")  //.csv directly load data
  //not neccessary to give load
  .load()

  import spark.implicits._
  val ordersDs =ordersDf.as[OrderData]
  ordersDs.filter(x=>x.order_id<10).show
    //ordersDf.filter(x=>x.order_ids<10)     ///try for checking
  //Dataset[Row]=>Dataframes
  //it shows error during runtime it is not compiletime safety
  scala.io.StdIn.readLine()
  spark.stop()
}