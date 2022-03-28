import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.Console._
import queries._


object main extends App {

  //<editor-fold desc="Spark Session = spark, SparkContext = sc">
  System.setProperty("hadoop.home.dir", "C:\\hadoop") //gets where hadoop is stored on computer
  Logger.getLogger("org").setLevel(Level.ERROR) //hides all non-error messages during runtime.
  val spark = SparkSession
    .builder
    .appName("Jack Nguyen Project 1")
    .config("spark.master", "local") //using data on local machine
    .enableHiveSupport() //enables hive queries for spark session
    .getOrCreate() //makes the session
  spark.sparkContext.setLogLevel("ERROR") //hides all non-error messages during runtime.
  val sc = spark.sparkContext

  spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("Set hive.exec.dynamic.partition.mode=nonstrict")
  spark.sql("set hive.enforce.bucketing = true")
  //</editor-fold>

  //<editor-fold desc="steamdata table creation">

  //spark.sql("DROP TABLE IF EXISTS steamData")
  val df = spark.sql("create table IF NOT EXISTS steamData(month_year STRING, Avg_Players FLOAT, gain FLOAT, percent_gain STRING, Peak_Players INT, url STRING, date DATE, name STRING) " +
    "row format delimited fields terminated by ','")
  df.write.partitionBy("name")
  df.write.bucketBy(10, "month_year")
  //spark.sql("LOAD DATA LOCAL INPATH 'Valve_Player_Data.csv' INTO TABLE steamData")

  //</editor-fold>

  println(s"${BOLD}Welcome to the Steam Player Data Analyzer, where the top 100 Steam Games from 2012-2021 are queried!$RESET\n\n")
  var isAdmin = false; var isBasic = false


  //spark.sql("SELECT * FROM steamData").show()
/*  val test = spark.sql("select MAX(peak_players) from steamdata where month_year = 'l 2021'")
  if (test.head().anyNull) {
    println("Invalid input")
  }
  else {
    test.show()
  }*/

  //can save csv into a dataframe and use spark functions or perform spark sql queries using views.
  /*val Bev_ConscountA=spark.read.csv("hdfs://localhost:9000/user/hive/warehouse/Bev_ConscountA.txt").toDF()
  Bev_ConscountA.createOrReplaceTempView("ConsA")
  spark.sql("SELECT * FROM ConsA").show()*/


}
