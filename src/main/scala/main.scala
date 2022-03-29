import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.Console._
import queries._
import CRUD._


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

  //<editor-fold desc="steamData and userAccounts table creation">

  //spark.sql("DROP TABLE IF EXISTS steamData")
  val dfSteam = spark.sql("create table IF NOT EXISTS steamData(month_year STRING, Avg_Players FLOAT, gain FLOAT, percent_gain STRING, Peak_Players INT, url STRING, date DATE, name STRING) " +
    "row format delimited fields terminated by ','")
  dfSteam.write.partitionBy("name")
  dfSteam.write.bucketBy(10, "month_year")
  //spark.sql("LOAD DATA LOCAL INPATH 'Valve_Player_Data.csv' INTO TABLE steamData")

  spark.sql("DROP TABLE IF EXISTS userAccounts")
  spark.sql("DROP TABLE IF EXISTS usersTemp")
  val dfAccounts = spark.sql("create table IF NOT EXISTS userAccounts(username STRING, password STRING, permissionType STRING) " +
    "stored as orc")

  //</editor-fold>

  println(s"${BOLD}Welcome to the Steam Player Data Analyzer, where the top 100 Steam Games from 2012-2021 are queried!$RESET\n")

  var isAdmin = false; var isBasic = false; var currentUN = ""; var currentPW = ""




}
