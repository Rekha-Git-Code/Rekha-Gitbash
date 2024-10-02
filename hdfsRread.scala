import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import saveJoinTable.spark


object hdfsRread extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "4join")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)

    
    .enableHiveSupport()
    .getOrCreate()
  //define Schema  //if it is a avro,parquet file we need not to define scahema

    //default mode permissive mode
    //.option("mode", "DROPMALFORMED")
    //.option("mode", "FAILFAST")

  //val Schema1 = "customer_id Int,first_name String"
  val readDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema",true)
    //.schema(Schema1)
    .option("path", "hdfs://192.168.126.128:8020/user/cloudera/test_folder/ITC_Contact")
    .load
  readDf.show()
  spark.stop()
}
//define schema//programatic approach
