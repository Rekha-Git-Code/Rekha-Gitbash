import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object repartitionCoalesce extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.spark-project").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sc = new SparkContext("local[*]", "repartition & coalesce")

  val input = sc.textFile("D:/Big data/biglog.txt")//.repartition(20)
  println(input.getNumPartitions)
  // val input1=input.repartition(20)
  // println(input1.getNumPartitions)
  // val mappedInput = input.map(x => {
  // val columns = x.split(":")
  //(columns(0),columns(1))
  // })
  val mappedInput = input.map(x => {
    val columns = x.split(":")
    (columns(0), 1)
  })
//mappedInput.collect.foreach(println)
  val results= mappedInput.reduceByKey(_+_)//.coalesce(3)
  //val results= mappedInput.reduceByKey(_+_).repartition(2)

  results.collect.foreach(println)
  println(results.getNumPartitions)

  results.saveAsTextFile("D:/Big data/Scala_testresultcoalesce6")

  scala.io.StdIn.readLine()

}

