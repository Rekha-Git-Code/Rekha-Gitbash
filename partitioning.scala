import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object partitioning extends App{

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

  val ordersDf=spark.read
    .format("csv")
    .option("header",true)
    .option("path","D:/Big data/orders.csv")
    .load

  print("orderdf has=" +ordersDf.rdd.getNumPartitions+"\n")
//inside the folder 4 files
  //  ordersRep.write
  ordersDf.write
    .format("csv")
    //partitionby //may be use two columns
    .partitionBy("order_status")
    .option("maxRecordsPerFile",2000)
    .mode(SaveMode.Overwrite)
    .option("path","D:/Big data/partitionby")
    .save()

  //ordersDf.show()
  // ordersDf.printSchema()
  //scala.io.StdIn.readLine()
  // spark.stop()
}