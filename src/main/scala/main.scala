import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.Console._
import queries._
import CRUD._
import scala.io.StdIn
import scala.util.control.Breaks._


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
  /*val dfSteam = spark.sql("create table IF NOT EXISTS steamData(month_year STRING, Avg_Players FLOAT, gain FLOAT, percent_gain STRING, Peak_Players INT, url STRING, date DATE, name STRING) " +
    "row format delimited fields terminated by ','")
  dfSteam.write.partitionBy("name")
  dfSteam.write.bucketBy(10, "month_year")*/
  //spark.sql("LOAD DATA LOCAL INPATH 'Valve_Player_Data.csv' INTO TABLE steamData")

  spark.sql("DROP TABLE IF EXISTS userAccounts")
  spark.sql("DROP TABLE IF EXISTS usersTemp")
  val dfAccounts = spark.sql("create table IF NOT EXISTS userAccounts(username STRING, password STRING, permissionType STRING) " +
    "stored as orc")
  createAccountHidden("jack", "test", "admin", spark)
  createAccountHidden("ERiC", "test", "basic", spark)
  createAccountHidden("Nick", "test", "basic", spark)

  //</editor-fold>

  var input1 = 0
  val menu1 =
    s"""
       |Please choose an option:
       |1. Login
       |2. Create New Account
       |3. Quit
       |""".stripMargin
  val aMenu =
    s"""
       |Please choose an option:
       |1. Use Queries
       |2. Manage Users
       |3. Change Username
       |4. Change Password
       |5. Log Out
       |""".stripMargin
  val bMenu =
    s"""
       |Please choose an option:
       |1. Use Queries
       |2. Change Username
       |3. Change Password
       |4. Log Out
       |""".stripMargin
  val qMenu =
    s"""
       |Please choose an option:
       |1. Game with Highest Player Count
       |2. Game with Lowest Player Count (at least 10000 players)
       |3. Top 10 Games with Current Highest Player Count (September 2021)
       |4. Top 10 Games with Highest Players in X Month and Year
       |5. Top 10 Month-Year that had the highest player count across Steam
       |6. Top 10 Month-Year with the highest average players for X Game
       |7. Exit
       |""".stripMargin
  val mMenu =
    s"""
       |Please choose an option:
       |1. Delete User
       |2. Update Privilege of User
       |3. Exit
       |""".stripMargin

  println(s"${BOLD}Welcome to the Steam Player Data Analyzer, where the top 100 Steam Games from 2012-2021 are queried!$RESET\n")
  breakable {
    while (input1 != 3) {
      println(menu1)
      input1 = StdIn.readInt()
      input1 match {
        case 1 => login()
        case 2 => createAccount("basic", spark)
        case 3 => println("Exiting app...")
        case _ => println("Invalid input!")
      }
    }
  }
  println("Thank you for using the Steam Player Data Analyzer! Have a great day!")

  def login (spark: SparkSession = spark) : Unit = {
    var UN = StdIn.readLine("Please enter your username:\n")
    var checkIfExist = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    while (checkIfExist.isEmpty) {
      UN = StdIn.readLine(s"$UN does not exist! Please enter another username:\n")
      checkIfExist = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    }
    var PW = StdIn.readLine("Please enter your password (case-sensitive):\n")
    var checkPW = spark.sql(s"select password from userAccounts " +
      s"where lower(username) = '${UN.toLowerCase}' and password = '$PW'")
    while (checkPW.isEmpty) {
      PW = StdIn.readLine(s"Password is incorrect! Please try again:\n")
      checkPW = spark.sql(s"select password from userAccounts " +
        s"where lower(username) = '${UN.toLowerCase}' and password = '$PW'")
    }
    val permission = spark.sql(s"select permissionType from userAccounts " +
      s"where lower(username) = '${UN.toLowerCase}' and password = '$PW'")
    val checkPermission = permission.head().getString(0)
    checkPermission match {
      case "admin" => println(s"Welcome $UN!"); adminMenu(UN, checkPermission)
      case "basic" => println(s"Welcome $UN!"); basicMenu(UN, checkPermission)
      case _ => println("Account error! Please contact Customer Support!")
    }
  }

  def adminMenu (UN: String, permission: String, menu: String = aMenu, spark: SparkSession = spark) : Unit = {
    var input = 0
    println("Welcome to the Admin Menu. Please enter a valid number to select an option:")
    breakable {
      while (input != 5) {
        println(menu)
        input = StdIn.readInt()
        input match {
          case 1 => queryMenu()
          case 2 => manageUserMenu()
          case 3 => updateUserName(UN, permission, spark); println("Logging out to update username in session..."); break()
          case 4 => updatePassword(UN, permission, spark)
          case 5 => println("Logging out...")
          case _ => println("Invalid input!")
        }
      }
    }
  }

  def basicMenu (UN:String, permission: String, menu: String = bMenu, spark: SparkSession = spark) : Unit = {
    println("Welcome to the User Menu. Please enter a valid number to select an option:")
    var input = 0
    breakable {
      while (input != 4) {
        println(menu)
        input = StdIn.readInt()
        input match {
          case 1 => queryMenu()
          case 2 => updateUserName(UN, permission, spark); println("Logging out to update username in session..."); break()
          case 3 => updatePassword(UN, permission, spark)
          case 4 => println("Logging out...")
          case _ => println("Invalid input!")
        }
      }
    }
  }

  def queryMenu (menu: String = qMenu, spark: SparkSession = spark) : Unit = {
    var input = 0
    while (input != 7) {
      println(menu)
      input = StdIn.readInt()
      input match {
        case 1 => queryHighestPeak(spark)
        case 2 => queryLowestPeak(spark)
        case 3 => queryCurrentHighest(spark)
        case 4 => queryHighestInX(spark)
        case 5 => queryHighestPlayerMonth(spark)
        case 6 => queryTopAverageInGame(spark)
        case 7 => println("Exiting...")
        case _ => println("Invalid input!")
      }
    }
  }

  def manageUserMenu (menu: String = mMenu, spark: SparkSession = spark) : Unit = {
    var input = 0
    while (input != 3) {
      println(menu)
      input = StdIn.readInt()
      input match {
        case 1 => deleteUser(spark)
        case 2 => updatePrivilege("admin", spark)
        case 3 => println("Exiting...")
        case _ => println("Invalid input!")
      }
    }
  }

}
