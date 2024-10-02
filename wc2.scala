import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object wc2 extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.spark-project").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "RDD-Example")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()
  //mapping fields using scala functions
  /*def parseline(x: String) = {
    val fields = x.split(",")
    val id = fields(0)
    val discount = fields(5).toFloat
    (id, discount)
  }*/
  val RDD1 = spark.sparkContext.textFile("D:/Big data/orders.csv")
  RDD1.collect.foreach(println)

  //take two colums order_id,discount
  val RDD2 = RDD1.map(x => (x.split(",")(0), x.split(",")(6)))
  RDD2.collect.foreach(println)
  //val RDD2 =RDD1.map(parseline)

  //RDD2.take(5).foreach(println)
  val RDD3 = RDD2.reduceByKey((x, y) => x + y)
  RDD3.collect.foreach(println)
    val RDD4 = RDD3.sortBy(x => x._1)
    println("Actions===>foreach")
  RDD4.collect.foreach(println)
  println("Actions===>count")
  println(RDD4.count())
  println("Actions===>countbyvalue")
  RDD4.countByValue().foreach(println)
  println("Actions===>min")
  println(RDD4.min())
  println("Actions===>max")
  println(RDD4.max())

  spark.stop()
}