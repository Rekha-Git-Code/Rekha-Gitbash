import joinTwo.{Schema2, resultDf, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object testjoin extends App {

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

  val Schema1 = "order_id Int,customer_id Int,order_status Int,order_date Date," +
    "required_date Date,shipped_date Date,store_id Int,staff_id Int"

  val ordersDf = spark.read
    .format("CSV")
    .option("header", true)
    .schema(Schema1)
    .option("path", "D:/Big data/orders.csv")
    .load
  ordersDf.show()

  val Schema2 = "customer_id Int,first_name String,last_name String,phone Int,email String," +
    "street String,city String,state String,zip_code Int"

  val customersDf = spark.read
    .format("csv")
    .option("header", true)
    .schema(Schema2)
    .option("path", "D:/Big data/customers.csv")
    .load
  customersDf.show()
  customersDf.printSchema()

  val joinDf = ordersDf.as("o").join(customersDf.as("c"), ordersDf.col("order_id") ===
  customersDf.col("customer_id"), "inner")
    .select("o.order_id","o.shipped_date","c.first_name", "c.email")
  joinDf.show()
  joinDf.createOrReplaceTempView("jointable")
  val resultDf = spark.sql("select * from jointable where first_name like 'S%' ")
  resultDf.show()
  spark.sql("create database if not exists retail4")

  resultDf.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .bucketBy(5,"order_id")
    .sortBy("order_id")
    .saveAsTable("retail4.buckettable")

  spark.catalog.listTables("retail4").show()
  spark.stop()



}
