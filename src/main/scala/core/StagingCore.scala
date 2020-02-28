package core

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object StagingCore {

  /**
    * Create the Hub and Sat tables for the staging stage.
    *
    * @param spark           : Spark session
    * @param resultPath      : HDFS path for store the table
    * @param originTable     : Original Hive/Impala table from where the Hub and Sat tables should be created
    * @param tableSchema     : Name of the schema for the Hub and Sat tables
    * @param hubTableName    : Name of the Hub table
    * @param satTableName    : Name of the Sat table
    * @param sidName         : Name of the SID column
    * @param originRegistro  : Name of the data source of the data
    * @param keyColumns      : List of columns for create the SID
    * @param hubFinalColumns : List of columns that must contain the Hub table
    * @param satFinalColumns : List of columns that must contain the Sat table
    * @return
    */
  def createHubSat(spark: SparkSession, resultPath: String, originTable: String, tableSchema: String,
                   hubTableName: String, satTableName: String, sidName: String, originRegistro: String,
                   keyColumns: List[String], hubFinalColumns: List[String], satFinalColumns: List[String]): DataFrame
  = {
    val df = spark.sql(s"SELECT * FROM $originTable")
      .withColumn(s"$sidName", md5(concat_ws("||", keyColumns.map(col): _*)))
      .withColumn("LoadDateTime", current_timestamp())
      .withColumn("ResourceSource", lit(s"$originRegistro"))

    // Create Staging HUB
    val dfHub = df.select(hubFinalColumns.map(col): _*)
    Utils.storeDf(spark, dfHub, hubTableName, tableSchema, resultPath + hubTableName)
    dfHub.unpersist()

    // Create Staging SAT
    val dfSat = df.select(satFinalColumns.map(col): _*)
    Utils.storeDf(spark, dfHub, satTableName, tableSchema, resultPath + satTableName)
    dfSat.unpersist()

    df
  }

  /**
    * Create a Link between two Hubs for the staging stage
    *
    * @param spark            : Spark session
    * @param resultPath       : HDFS path for store the table
    * @param dfHub1           : First spark dataframe (The bigger must be pass as the first dataframe)
    * @param dfHub2           : Second spark dataframe
    * @param conditions       : Conditions for the join between the dataframes
    * @param sidName          : Name of the SID column
    * @param linkTableName    : Name of the Link table
    * @param tableSchema      : Name of the schema for the Link table
    * @param keyColumns       : List of columns for create the SID
    * @param linkFinalColumns : List of columns that must contain the Link table
    * @param typeJoin         : By default it's a inner join, the values can be: inner, outer, left, right.
    * @param smallJoin        : By default, do a Broadcast of the second dataframe. Must be false if the df it's big.
    * @return
    */

  def createLink(spark: SparkSession, resultPath: String, dfHub1: DataFrame, dfHub2: DataFrame,
                 conditions: Column, sidName: String, linkTableName: String, tableSchema: String,
                 keyColumns: List[String], linkFinalColumns: List[String], typeJoin: String = "inner",
                 smallJoin: Boolean = true): DataFrame = {


    // If the flag smallJoin it's True, then the Dataframe dfHub2 must be mark with the function broadcast
    val dfTemp = if (smallJoin) dfHub1.join(broadcast(dfHub2), conditions, typeJoin) else dfHub1.join(dfHub2,
      conditions, typeJoin)

    val dfLink = Utils.dropDuplicates(dfTemp)
      .withColumn(s"$sidName", md5(concat_ws("||", keyColumns.map(col): _*)))
      .select(linkFinalColumns.map(col): _*)

    Utils.storeDf(spark, dfLink, linkTableName, tableSchema, resultPath + linkTableName)
    dfLink
  }


}
