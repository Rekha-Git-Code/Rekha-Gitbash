import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object parallize {
  def main(args: Array[String]): Unit = {
    //config
    val sparkConf = new SparkConf()


    sparkConf.set("spark.app.name", "Example")
    sparkConf.set("spark.master", "local[*]")
    // create SparkSession
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // create an RDD with some data
    val rdd1 = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5,5,6))
rdd1.collect.foreach(println)
    val rdd2 = spark.sparkContext.parallelize(List(6,7,8,9,10))
    rdd2.collect.foreach(println)
    val rdd3 = spark.sparkContext.parallelize(Array(11,12,13,14,15))
    rdd3.collect.foreach(println)

    val input =  spark.sparkContext.parallelize(List("1,aaa", "2,bbb", "3,bbb"))
    val word1= input.flatMap(x => x.split(","))
    word1.collect.foreach(println)
    val word2 = input.map(x => x.split(",")(0))
    word2.collect.foreach(println)

    // apply map function to RDD to double each element
    val doubled = rdd1.map(x => x*2)

    // print the result
    doubled.collect().foreach(println)

    // stop the SparkSession
    spark.stop()
  }
}