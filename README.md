# Project1
## Project Description
The 2nd solo project for Revature. This project allows users to fetch data from a local CSV file with Steam Player Data for the top 100 games on Steam from 2012-2021 and performing analytical queries using Spark's API in IntelliJ. It also includes a user-login system with BASIC and ADMIN users that each have their own set of privileges.

## Technologies Used
- Java - version 1.8.0_311
- Scala - version 2.12.0
- Spark - version 3.2.1
- Spark SQL - version 3.2.1
- Hive - version 3.2.1
- HDFS - version 3.3.0
- Git + GitHub

## Features
- User Interface including login, basic user creation, and query selection.
- Restricted access for basic users, complete access for admin users.
- Fetching and reporting data from Steam Player Data using analytical Spark SQL Queries.
- Implemented partitioning and bucketing on main data table to enable more efficient querying and data retrieval.
- Input validation for usernames, passwords, and queries.
- Admin Capability of promoting Basic Users to Admin, Deleting Basic Users.
- Implementation of changing username and password for all users.
- Passwords stored and encrypted within an internal database.

## Getting Started (Windows 10 and above)
- Download and install Git:
  - https://git-scm.com/download/win
- Run the following Git command in Command Prompt or PowerShell to clone the project to your computer.
  - git clone git@github.com:Jackeywawa/Project1.git
- Install Java
  - https://www.oracle.com/java/technologies/javase/javase8u211-later-archive-downloads.html
- Install Scala
  - https://www.scala-lang.org/download/scala2.html
- Install IntelliJ and download Scala Plugin within IntelliJ
  - https://www.jetbrains.com/idea/download/download-thanks.html?platform=windows&code=IIC
  - Go to File -> Settings -> Plugins -> Search for Scala Plugin -> Install 

## Usage
- Right-click on the project reopisitory, select "Open Folder as IntelliJ IDEA Community Edition Project", and then run it by selecting "Run -> Run" at the top.
- General Usage
  - You can login using the default admin account (UN: jack, PW: test) or make a new basic user account. Usernames are not case-sensitive but passwords are.
  - After successfully logging in, you will be in a User Menu with different options based on whether you are a basic or admin user.
- Admin Menu
  - Query Menu with 6 Different Query Options to analyze Steam Player Data.
  - Manage Users with 2 Options: Deleting Existing Users and Promoting Basic Users to Admin.
  - Change Username: Allows user to change their username after entering their password. User will be logged out once they change their username.
  - Change Password: Allows user to change their password after entering their current password. Users are advised but not forced to enter a strong password.
- Basic Menu
  - Same options as Admin Menu minus being able to Manage Users.

## License
This project uses the following license: The Unlicense
