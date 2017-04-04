# StataMySQLPlugin
A MySQL plug-in for Stata

The plugin allows Stata users to run SELECT queries against MySQL databases and use the results in Stata.
The plugin uses the official MySQL JDBC driver, it requires Stata v13 or above to run.

## Installation
1. Download the alphacruncher_stataMySQLPlugin_1.0.jar from GitHub, and copy it to the local private ADO folder (```C:\ado\private\```)
2. Start Stata and run
```
javacall com.alphacruncher.StataMySQLPlugin help
```
to check if the plug-in is loaded.

## User's Guide
Currently only SELECT queries are supported, as the plug-in is intended to be used to query non-volatile reference data.

1. First, you need to create a password file on the local file system, preferably under ```C:\Users\<your user>```, which should contain the following:
   ```
   user=<your username>
   password=<your password>
   ```
   Do not enclose your username and password in quotes. This file is needed so you wouldn't have your password in clear text in the Stata session.

2. Initialize the MySQL driver by running the following command, specifying the JDBC URL of the database you wish to connect to and the password file created in the previous step:
   ```
   javacall com.alphacruncher.StataMySQLPlugin initialize, args("jdbc:mysql://host:port/db"  "C:\\Users\\username\\username.pass")
   ```
   The JDBC URL has the following format: jdbc:mysql://host:port/database
   This command only needs to be run once per session, or when you wish to connect as another user or connect to another database.

3. Run your query against the database with the following command:
   ```
   javacall com.alphacruncher.StataMySQLPlugin query, args("SELECT * FROM ... LIMIT 100 ")
   ```
   New Stata variables will be created with the names and types of the columns of the result table, and the records will be appended as new observations to the existing observations.
   Date/timestamp columns will be stored in string variables, in the following format: YYYY-mm-dd HH:mm:ss.SSSSSSZ

4. Use the print command to display the results of your query.
For performance reasons, it is recommended to limit the number of results retrieved by adding LIMIT 100 to the end of the SELECT query you wish to execute.

## Release notes for v1.1 - 2017.04.04

* Based on Stata 14, observation count now a Long value.
* Changed the ResultSet to a read-only, forward scrolling ResultSet to support querying large tables.
* MySQL Connector/J version update to 5.1.41.
