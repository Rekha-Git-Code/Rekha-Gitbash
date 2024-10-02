import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source

object broadcast_Variable extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.spark-project").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  //val set1 = Set("are","is")  //step1

  val sc = new SparkContext("local[*]", "remove boaring words")
  //var nameSet = sc.broadcast(set1)   // step1
  var nameSet = sc.broadcast(Set("are","is"))  //instead of this we can use step1(2times above)
  println("boaringwords=" + nameSet.value)
  val input = sc.textFile("C:/Users/USER/Desktop/testaccum.txt")

  val words = input.flatMap(x => x.split(" ")) // (seperate each word by space)   //flatmap
  words.collect.foreach(println)
  val wordMap = words.map(x => (x, 1))
  wordMap.collect.foreach(println)
  //println("================\n")
  //val filteredRdd = wordMap.filter(x => !nameSet.value(x._1))
  val filteredRdd = wordMap.filter( x => !nameSet.value(x._1))
  //val filteredRdd = wordMap.filter((x=>(x._1)!= (nameSet.value)))
  filteredRdd.collect.foreach(println)
  // println("================\n")
  //val total = wordMap.reduceByKey(_+_)
  val total = filteredRdd.reduceByKey(_ + _)

  total.collect.foreach(println)
}
