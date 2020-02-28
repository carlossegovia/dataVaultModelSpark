package models

import core.StagingCore
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object DataVaultModelExample {
  val ResultPath = "someHdfsPath/warehouse/bds/"
  val BDSShecma = "bds"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Model Test Datavault")
      .getOrCreate()

    // Example of how to create automatically HUB, SAT and Links tables.
    // Create the HUB's and SAT's of two models
    val dfAirport = createAirportHubSat(spark)
    val dfFlight = createFlightHubSat(spark)

    // Create the Link between Airport and Flight
    createLinkAirportFlight(spark, dfAirport, dfFlight)

    spark.stop()
  }

  /**
    * Define the parameters for create the Hub and Sat of Airport
    *
    * @param spark : Spark session
    * @return
    */
  def createAirportHubSat(spark: SparkSession): DataFrame = {
    val hubTableName = "HubAirport"
    val satTableName = "SatelliteAirport"
    val sidName = "HubAirportHashKey"
    val origenRegistro = "FlightSearchDb.dbo.Airport"
    val originTable = "HiveFlightDb.datalake.Airport"

    val keyColumns: List[String] = List("AirportCode")
    val commonColumns: List[String] = List(sidName, "LoadDateTime", "ResourceSource")
    val hubFinalColumns: List[String] = commonColumns ++ List("AirportCode")
    val satFinalColumns: List[String] = commonColumns ++ List("AiportName", "IANATimeZone", "LocationId", "City",
      "State", "PostalCode", "CountryCode")

    StagingCore.createHubSat(spark, ResultPath, originTable, BDSShecma, hubTableName, satTableName, sidName,
      origenRegistro, keyColumns, hubFinalColumns, satFinalColumns)
  }

  /**
    * Define the parameters for create the Hub and Sat of Flight
    *
    * @param spark : Spark session
    * @return
    */
  def createFlightHubSat(spark: SparkSession): DataFrame = {
    val hubTableName = "HubFlight"
    val satTableName = "SatelliteFlight"
    val sidName = "HubFlightHashKey"
    val origenRegistro = "Kafka"
    val originTable = "HiveFlightDb.datalake.Flight"

    val keyColumns: List[String] = List("AirlineCode", "FlightNumber", "ScheduledDepartureTime")
    val commonColumns: List[String] = List(sidName, "LoadDateTime", "ResourceSource")
    val hubFinalColumns: List[String] = commonColumns ++ List("AirlineCode", "FlightNumber", "ScheduledDepartureTime")
    val satFinalColumns: List[String] = commonColumns ++ List("FlightId", "AirlineName", "DepartureAirportCode",
      "ArrivalAirportCode", "ScheduledDepartureTime", "ScheduleArrivalTime")

    StagingCore.createHubSat(spark, ResultPath, originTable, BDSShecma, hubTableName, satTableName, sidName,
      origenRegistro, keyColumns, hubFinalColumns, satFinalColumns)
  }

  /**
    * Define the parameters for create the Link between Flight and Airport
    *
    * @param spark     : Spark session
    * @param dfFlight  : Flight dataframe
    * @param dfAirport : Airport dataframe
    * @return
    */
  def createLinkAirportFlight(spark: SparkSession, dfFlight: DataFrame, dfAirport: DataFrame): DataFrame = {
    val sidName = "LinkFlightAirportHashKey"
    val linkTableName = "linkFlightAirport"
    val keyColumns: List[String] = List("HubFlightHashKey", "HubAirportHashKey")

    val linkFinalColumns: List[String] = List(sidName, "LoadDateTime", "ResourceSource", "HubFlightHashKey",
      "HubAirportHashKey", "AirlineCode", "FlightNumber", "ScheduledDepartureTime", "AirportCode")

    val conditions: Column = dfFlight("AirlineCode") <=> dfAirport("AirlineCode")

    StagingCore.createLink(spark, ResultPath, dfFlight, dfAirport, conditions, sidName, linkTableName, BDSShecma,
      keyColumns, linkFinalColumns)
  }
}
