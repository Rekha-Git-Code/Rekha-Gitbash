import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.util.LongAccumulator
object accumVariable extends App{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.spark-project").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val sc =new SparkContext("local[*]","wordCount")
  //val myRdd = sc.textFile("C:/Ranjini/Ranjini/Dataset/Dataset2/accum.txt")`


  val myRdd = sc.textFile("C:/Users/USER/Desktop/testaccum.txt")
  myRdd.collect.foreach(println)
  val myAccumulator= sc.longAccumulator("blank lines accumulator")
  //val flatRdd =myRdd.flatMap(_.split(" "))
  // Define a function to count lines with the word "spark"
  /* def countLinesWithWordbig(x: String): Unit = {
     if (x==("big")) {
       myAccumulator.add(1)
     }
   }
   */

  // Apply the function to each line in the text file
  //  myRdd.foreach(countLinesWithWordbig)
  //myRdd.foreach(x=>if(x.contains("you")) myAccumulator.add(1))
  myRdd.foreach( x => if(x== "" ) myAccumulator.add(1))
  // Print the final count
  //println("Number of lines containing 'you': " + myAccumulator.value)
  println("Empty lines="+myAccumulator.value)
  //println("Number of blank lines : " + myAccumulator.value)

}




