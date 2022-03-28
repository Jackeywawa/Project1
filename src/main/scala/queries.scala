import org.apache.spark.sql.SparkSession

object queries extends App {
  //month_year STRING, Avg_Players FLOAT, gain FLOAT, percent_gain STRING, Peak_Players INT, url STRING, date DATE, name STRING
  def queryHighestPeak(spark: SparkSession) : Unit = {
    println("Highest player count:")
    spark.sql("select name as Game, peak_players as Players, month_year from steamData " +
      "where peak_players = (select(max(peak_players)) from steamData)").show()
    //maybe change to show top 10 results
  }
  def queryLowestPeak(spark: SparkSession) : Unit = {
    println("Lowest player count:")
    spark.sql("select distinct name as Game, peak_players as Players, month_year from steamData " +
      "where peak_players = (select(min(peak_players)) from steamData where peak_players > 10000)").show(1)
  }
  def queryCurrentHighest(spark: SparkSession) : Unit = {
    println("Games with latest highest player count (September 2021):")
    spark.sql("select distinct name as Game, peak_players as Players from steamData " +
      "where month_year = 'September 2021' " +
      "order by peak_players desc").show(10, false)
  }
  def queryHighestInX(spark: SparkSession, input: String) : Unit = {
    println(s"Games with highest player count in $input:")
    /*spark.sql("select name, peak_players, substring(month_year, length('month_year')-3, 4) as Year from steamData " +
      s"where Year = '$year' order by peak_players desc").show(1, false)*/
    spark.sql("select name as Game, peak_players as Players, month_year as Date from steamData " +
      s"where month_year = '$input' order by peak_players desc").show(10, false)
  }
  def queryHighestPlayerMonth(spark: SparkSession) : Unit = {
    println("Month Year with the highest total player count across all games:")
    /*spark.sql("select month_year from steamData where Peak_Players = " +
      "(select max((select sum(peak_players) from steamData group by month_year)) from steamData)").show()*/
    spark.sql("select month_year as Date, sum(peak_players) as Total_Players from steamData group by month_year order by Total_Players desc").show(10, false)
  }
  def queryTopAverageInGame(spark: SparkSession, input: String) : Unit = {
    println(s"Highest Average Player Count for $input:")
    spark.sql("select ceil(Avg_players) as Average, month_year as Date from steamData " +
      s"where name = '$input' order by Avg_players desc").show(10, false)
  }
}
