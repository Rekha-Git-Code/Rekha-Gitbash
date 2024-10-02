import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object scalaSql extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.spark-project").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val Schema1 = "order_id Int, order_date string," +
    "order_customer_id int, order_status string"

  val ordersDf=spark.read
    .format("csv")
    //.option("header",true)
    //.option("inferSchema",true)
    .schema(Schema1)
    .option("path","D:/Big data/Spark/orders.csv")
    .load()
  ordersDf.show()
  ordersDf.printSchema()
  ordersDf.createOrReplaceTempView("orders")

  val resultDf = spark.sql("select order_status,count(*) as status_count from orders " +
    "group by order_status order by status_count desc")
  resultDf.show()
  scala.io.StdIn.readLine()
  spark.stop()
}
