import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object sqlExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[*]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

 val Schema1 = StructType(List(
    StructField("emp_ID", IntegerType),
    StructField("emp_NAME", StringType),
    StructField("DEPT_NAME", StringType),
    StructField("SALARY",IntegerType)))
  case class dataset1(emp_id: Int,emp_name: String,DEPT_NAME:String,SALARY:Double)
  val empDf=spark.read
    .format("csv")
    .option("header",true)
   // .option("inferSchema",true)
    .schema(Schema1)
    .option("path","C:/Ranjini/Ranjini/Dataset/Dataset3/emp.csv")
    .load
// empDf.show()
 // empDf.printSchema()
 empDf.filter(" DEPT_NAME='Admin' ").show//working
 //empDf.map(x=>(x,1)).show//not working

  //empDf.select("dept_name")
    //.groupBy("dept_name")
    //.count().show()

 //import spark.implicits._

 //val Ds1 = empDf.as[dataset1]

 // Ds1.filter(x => x.SALARY > 3000)
//Ds1.map(x=>(x,1)).show()
   //Ds1.createOrReplaceTempView("emp")

  //val resultDs =spark.sql("select * from emp")
 //val resultDf =spark.sql("select count(*) from emp")
  //val resultDf=spark.sql("select emp_id,emp_name from emp where salary>4000")
  //val resultDf=spark.sql("select dept_name,AVG(salary)as salary from emp group by dept_name")
  //val resultDf=spark.sql("select dept_name,avg(salary)as salary " +
  // "from emp " +
  // "group by dept_name"
  //resultDs.show
  //scala.io.StdIn.readLine()
  spark.stop()
}


