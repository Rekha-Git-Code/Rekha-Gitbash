
//use all functions in this program
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType,FloatType}
import org.apache.spark.sql.functions._
//import savedatabasetable.spark

object saveJoinTable extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "4join")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
  //define Schema  //if it is a avro,parquet file we need not to define scahema
  val Schema1 = "order_id Int,customer_id Int,order_status Int,order_date Date," +
    "required_date Date,shipped_date Date,store_id Int,staff_id Int"
  val ordersDf = spark.read
    .format("csv")
    .option("header",true)
    //.option("inferSchema",true)//Automatically inferschema
    .schema(Schema1)
    //default mode permissive mode
    //.option("mode", "DROPMALFORMED")
    //.option("mode", "FAILFAST")
    .option("path", "C:/Ranjini/Ranjini/Dataset/CSVfolder/orders.csv")
    .load
  ordersDf.show()
  val Schema2 = "customer_id Int,first_name String,last_name String,phone Int,email String," +
    "street String,city String,state String,zip_code Int"
  val customerDf = spark.read
    .format("csv")
    .option("header",true)
   //.option("inferSchema",true)
    .schema(Schema2)
    .option("path", "C:/Ranjini/Ranjini/Dataset/CSVfolder/customers.csv")
    .load
  customerDf.show()
  //define schema//programatic approach
 ////DDL
 // val Schema3 = "order_id Int,item_id Int,products_id Int,quantity Int,list_price Float,discount Float"
 val Schema3 = StructType(List(
    StructField("order_id", IntegerType),
    StructField("item_id", IntegerType),
    StructField("products_id", IntegerType),
      StructField("quantity", IntegerType),
      StructField("list_price", FloatType),
      StructField("discount", IntegerType)))

  val orderitemsDf = spark.read
    .format("csv")
    .option("header", true)
    //.option("inferSchema",true)
    .schema(Schema3)
    .option("path", "C:/Ranjini/Ranjini/Dataset/CSVfolder/order_items.csv")
    .load
  orderitemsDf.show()
val Schema4 = "product_id Int,product_name String,brand_id Int,category_id Int,model_year Int,list_price Float"
  val productsDf = spark.read
    .format("csv")
    .option("header", true)
    //.option("inferSchema",true)
    .schema(Schema4)
    .option("path", "C:/Ranjini/Ranjini/Dataset/CSVfolder/products1.csv")
    .load
  productsDf.show


  val joinDf = ordersDf.as("o")
    .join(customerDf.as("c"),ordersDf.col("customer_id")=== customerDf.col("customer_id"),"inner")
    .join(orderitemsDf.as("oi"),ordersDf.col("order_id")===orderitemsDf.col("order_id"),"inner")
    .join(productsDf.as("p"),orderitemsDf.col("products_id")===productsDf.col("product_id"),"inner")


   //joinDf.show
    //withcolumn used for change column name
    .withColumn("new_list_price",orderitemsDf("list_price")*100)
  .select("o.order_id","o.order_date","c.first_name",
    "c.email","oi.products_id","oi.quantity","p.product_name","new_list_price")//.show
//----spark sql
  joinDf.createOrReplaceTempView("jointable")
  val resultDf = spark.sql("select * from jointable where first_name like 'D%' ")
  resultDf.show


//write the dataframe as table
  /*spark.sql("create database if not exists retail1")
  joinDf.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    //bucketing the table
      .bucketBy(4, "order_id")
    .sortBy("order_id")
    .saveAsTable("retail1.join4Table")
  //just show the database tables
  spark.catalog.listTables("retail1").show()*/
  //write the dataframe as files using partition
  joinDf.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    //.mode(SaveMode.Append)
    //.mode(SaveMode.Ignore)
    //.mode(SaveMode.ErrorIfExists)
    .partitionBy("first_name")
    .option("maxRecordsPerFile", 2000)
    .option("path","C:/dataset/joinpartition")
    .save()

  spark.stop()
}

