package Rekha

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object wc1 extends App {
  //The main method is automatically defined by the App trait.
  // def main(args:Array[String])
  // {}

  Logger.getLogger("org").setLevel(Level.ERROR)
  // if any error show me otherwise no need to show


  val sc = new SparkContext("local[*]", "wordCount")

  val input = sc.textFile("D:/spark_practice/datafile.txt")
  //input.collect.foreach(println)
  val words = input.flatMap(x => x.split(" ")) //narrow
  //words.collect.foreach(println)
  val wordMap = words.map(x => (x, 1)) //narrow
  //wordMap.collect.foreach(println)
  val finalCount = wordMap.reduceByKey((a, b) => a + b) //wide
  //finalCount.collect.foreach(println)
  val sortbyResult1 = finalCount.sortBy(x => x._1)
  //sort by first value   //wide
  //sortbyResult1.collect.foreach(println)

  //sortbyResult1.collect.foreach(println)
  sortbyResult1.saveAsTextFile("D:/spark_practice/deploy_result1")
  scala.io.StdIn.readLine()

  /*
  //finalCount.collect.foreach(println)
   */
  //finalCount.saveAsTextFile("C:/Ranjini/Ranjini/Dataset/wcresult2")
  /*val sortKeyResult =finalCount.sortByKey()//sort by key only   //wide
  sortKeyResult.collect.foreach(println)
  val sortbyResult1 = finalCount.sortBy(x=>x._1) //sort by first value   //wide
  sortbyResult1.collect.foreach(println)
  val sortbyResult2 = finalCount.sortBy(x => x._2)//sort by second value  //wide
  sortbyResult2.collect.foreach(println)
  val filterResult1 = finalCount.filter(x=>x._2>2)    //filter the result
  val filterResult2=finalCount.filter(x => x._1 == "you") //narrow
  filterResult2.collect.foreach(println)
  filterResult1.saveAsTextFile("C:/Ranjini/Ranjini/Dataset/wcresult1") //save result into the folder
  scala.io.StdIn.readLine()
  // this means program is still running not terminated
  //it will show DAG

   */
}
