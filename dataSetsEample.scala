import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.util.DropMalformedMode
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import java.sql
///read from datasources

object dataSetsEample extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  //define schema programmatic approach
  val Schema1 = StructType(List(
  StructField("id", IntegerType,false),
  StructField("name", StringType,false)))
 case class dataset1(id:Int, name :String)

  val Df1 = spark.read
    .format("csv")
    .option("header", true)
    .option("path","C:/Users/USER/Desktop/testfile.csv")
    //.option("inferSchema", true)
    .schema(Schema1)
    // .option("mode","DROPMALFORMED")
    //.option("mode","FAILFAST")
    .load

  import spark.implicits._

  val Ds1 = Df1.as[dataset1]
  //Ds1.filter(x => x.id == 1).show
  Ds1.filter("id == 1").show

  //Df1.printSchema()
  // Df1.show(false)

  //val Schema2 = "id Int,name String"
  //define schema string approach

  //val Df2 =spark.read
    //.format("csv")
    /*.option("header",true)
    .schema(Schema2)
    .option("path","C:/dataset/ex2.csv")
    .load
 // Df2.printSchema()
  //Df2.show(false)

     */
  spark.stop()
}