import org.apache.spark.sql.SparkSession
import scala.Console._
import scala.io.StdIn

object CRUD extends App{
  //username STRING, password STRING, permissionType STRING
  def createAccount(UN: String, PW: String, permission: String, spark: SparkSession) : Unit = {
    val checkIfUnique = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    if (checkIfUnique.isEmpty) {
      spark.sql(s"insert into userAccounts VALUES ('$UN','$PW','$permission')")
      if (permission == "basic")
      println("Account created!")
    }
    else {
      val retryUN = StdIn.readLine(s"$UN has already been taken! Please enter another username:\n")
      createAccount(retryUN, PW, permission, spark)
    }
  }

  def readAccounts(spark : SparkSession) : Unit = {
    spark.sql("select username as Username, password as Password, permissionType as Privileges from userAccounts" +
      " order by privileges, Username").show(false)
  }

  def updatePassword(UN: String, permission: String, spark: SparkSession) : Unit = {
    val oldPW = StdIn.readLine("Please enter your current password:\n")
    val checkPW = spark.sql(s"select password from userAccounts " +
      s"where lower(username) = '${UN.toLowerCase}' and password = '$oldPW'")
    if (!checkPW.isEmpty) {
      val newPW = StdIn.readLine("Please enter a new password: \n")
      createUserAccountsCopy(spark)
      spark.sql(s"insert into usersTemp select * from userAccounts where lower(username) != '${UN.toLowerCase}'")
      spark.sql("drop table userAccounts")
      spark.sql("alter table usersTemp rename to userAccounts")
      spark.sql(s"insert into table userAccounts VALUES ('$UN','$newPW','$permission')") //bug: will change UN to whatever was passed into function, shouldn't be an issue when using current username
      println(s"${BOLD}Password has been updated to $newPW!$RESET")
    }
    else {
      println(s"$oldPW is incorrect! Please try again...\n")
      updatePassword(UN, permission, spark)
    }
  }

  def updateUserName(UN: String, permission: String, spark: SparkSession) : Unit = {
    val PW = StdIn.readLine("Please enter your current password:\n")
    val checkPW = spark.sql(s"select password from userAccounts " +
      s"where lower(username) = '${UN.toLowerCase}' and password = '$PW'")
    if (!checkPW.isEmpty) {
      var newUN = StdIn.readLine("Please enter a new username: \n")
      var checkIfUnique = spark.sql(s"select username from userAccounts where lower(username) = '${newUN.toLowerCase}'")
      while (!checkIfUnique.isEmpty) {
        newUN = StdIn.readLine(s"$newUN is taken! Please enter another username: \n")
        checkIfUnique = spark.sql(s"select username from userAccounts where lower(username) = '${newUN.toLowerCase}'")
      }
      createUserAccountsCopy(spark)
      spark.sql(s"insert into usersTemp select * from userAccounts where lower(username) != '${UN.toLowerCase}'")
      spark.sql("drop table userAccounts")
      spark.sql("alter table usersTemp rename to userAccounts")
      spark.sql(s"insert into table userAccounts VALUES ('$newUN','$PW','$permission')")
      println(s"${BOLD}Username has been updated to $newUN!$RESET")
    }
    else {
      println(s"$PW is incorrect! Please try again...\n")
      updateUserName(UN, permission, spark)
    }
  }

  def deleteUser(spark: SparkSession) : Unit = {
    readAccounts(spark)
    var UN = StdIn.readLine("Please enter the user you wish to delete: \n")
    var checkIfExist = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    while (checkIfExist.isEmpty) {
      UN = StdIn.readLine(s"$UN does not exist! Please enter another username: \n")
      checkIfExist = spark.sql(s"select username from userAccounts where lower(username) = '${UN.toLowerCase}'")
    }
    createUserAccountsCopy(spark)
    spark.sql(s"insert into usersTemp select * from userAccounts where lower(username) != '${UN.toLowerCase}'")
    spark.sql("drop table userAccounts")
    spark.sql("alter table usersTemp rename to userAccounts")
    println(s"${BOLD}$UN has been deleted successfully!$RESET")
  }

  def createUserAccountsCopy(spark: SparkSession, table_name : String = "usersTemp") : Unit = {
    spark.sql(s"create table if not exists $table_name(username STRING, password STRING, permissionType STRING)" +
      s"stored as orc")
  }

  //obsolete functions
  /*  def updatePassword(UN: String, oldPW: String, permission: String, spark: SparkSession) : Unit = {
    val checkPW = spark.sql(s"select password from userAccounts " +
      s"where username = '$UN' and password = '$oldPW'")
    if (!checkPW.isEmpty) {
      val newPW = StdIn.readLine("Please enter a new password: \n")
      createUserAccountsCopy(spark)
      spark.sql(s"insert into userTemps select * from userAccounts where username != '$UN'")
      spark.sql("drop table userAccounts")
      spark.sql("alter table userTemps rename to userAccounts")
      spark.sql(s"insert into table userAccounts VALUES ('$UN','$newPW','$permission')")
    }
    else {
      val retryPW = StdIn.readLine(s"$oldPW is incorrect! Please try again...\n")
      updatePassword(UN, retryPW, permission, spark)
    }
  }*/
}
