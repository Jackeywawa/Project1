import scala.Console._
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object main extends App {
  System.setProperty("hadoop.home.dir", "C:\\hadoop") //gets where hadoop is stored on computer
  Logger.getLogger("org").setLevel(Level.ERROR) //hides all non-error messages during runtime.
  val spark = SparkSession
    .builder
    .appName("Jack Nguyen Project 1")
    .config("spark.master", "local") //using data on local machine
    .enableHiveSupport() //enables hive queries for spark session
    .getOrCreate() //makes the session
  println("created spark session")
  spark.sparkContext.setLogLevel("ERROR") //hides all non-error messages during runtime.

  spark.sql("DROP TABLE IF EXISTS newone");
  spark.sql("create table IF NOT EXISTS newone(id Int,name String) row format delimited fields terminated by ','");
  spark.sql("LOAD DATA LOCAL INPATH 'kv1.txt' INTO TABLE newone")
  spark.sql("SELECT * FROM newone").show()

  /*println(s"${BOLD}Welcome to the Steam Player Data Analyzer, where the top 100 Steam Games from 2012-2021 are queried!$RESET")
  var isAdmin = false; var isBasic = false
  var usernames = ArrayBuffer("jack")*/
}
