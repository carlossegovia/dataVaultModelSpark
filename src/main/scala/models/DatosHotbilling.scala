package models

import core.StagingCore
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object DatosHotbilling {
  val ResultPath = "/user/prueba/dwh_scala/"
  val BDSShecma = "BDS"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Scala Test DatosHotbilling")
      .getOrCreate()

    val dfHotbilling = createDatosHotbillingHubSat(spark)

    val dfCell = createCellHubSat(spark)

    createLinkHotbillingCell(spark, dfHotbilling, dfCell)

    spark.stop()
  }

  def createDatosHotbillingHubSat(spark: SparkSession): DataFrame = {
    val hubTableName = "hub_hotbilling_scala"
    val satTableName = "sat_hobtilling_scala"
    val sidName = "DATOS_HOTBILLING_SID"
    val origenRegistro = "TASACION.BILLING"
    val originTable = "datalake.ODS_DATOS_HOTBILLING"

    val keyColumns: List[String] = List("NUMERO_LINEA_A", "FECHA_INICIO", "CHARGING_ID", "RATING_GROUP",
      "SERVICE_IDENTIFIER", "RESULT_CODE", "FAILURE_HANDLING_CONTINUE", "RECORD_SEQUENCE_NUMBER", "CDR_FILE")

    val commonColumns: List[String] = List(sidName, "FECHA_REGISTRO_DV", "ORIGEN_REGISTRO_DV")

    val hubFinalColumns: List[String] = commonColumns ++ List("NUMERO_LINEA_A", "FECHA_INICIO", "FECHA_REGISTRO",
      "CHARGING_ID", "RATING_GROUP", "SERVICE_IDENTIFIER", "RESULT_CODE", "FAILURE_HANDLING_CONTINUE",
      "RECORD_SEQUENCE_NUMBER", "CDR_FILE")

    val satFinalColumns: List[String] = commonColumns ++ List("NUMERO_LINEA_A", "FECHA_INICIO", "FECHA_REGISTRO",
      "CHARGING_ID", "RATING_GROUP", "SERVICE_IDENTIFIER", "RESULT_CODE", "FAILURE_HANDLING_CONTINUE",
      "RECORD_SEQUENCE_NUMBER", "CDR_FILE")

    StagingCore.createHubSat(spark, ResultPath, originTable, BDSShecma, hubTableName, satTableName, sidName,
      origenRegistro, keyColumns, hubFinalColumns, satFinalColumns)
  }

  def createCellHubSat(spark: SparkSession): DataFrame = {
    val hubTableName = "hub_cell_scala"
    val satTableName = "sat_cell_scala"
    val sidName = "CELL_SID"
    val origenRegistro = "TECNICA"
    val originTable = "datalake.ODS_CELL"

    val keyColumns: List[String] = List("CELLID", "NAME")
    val commonColumns: List[String] = List(sidName, "FECHA_REGISTRO_DV", "ORIGEN_REGISTRO_DV")
    val hubFinalColumns: List[String] = commonColumns ++ List("CELLID", "NAME")
    val satFinalColumns: List[String] = commonColumns ++ List("LAC", "SITEID")

    StagingCore.createHubSat(spark, ResultPath, originTable, BDSShecma, hubTableName, satTableName, sidName,
      origenRegistro, keyColumns, hubFinalColumns, satFinalColumns)
  }

  def createLinkHotbillingCell(spark: SparkSession, dfHB: DataFrame, dfCell: DataFrame): DataFrame = {
    val sidName = "HOTBILLING_CELDA_SID"
    val linkTableName = "LNK_BDS_HOTBILLING_CELL_scala"
    val keyColumns: List[String] = List("NUMERO_LINEA_A", "FECHA_INICIO", "CHARGING_ID", "RATING_GROUP",
      "SERVICE_IDENTIFIER", "RESULT_CODE", "FAILURE_HANDLING_CONTINUE", "RECORD_SEQUENCE_NUMBER", "CDR_FILE",
      "CELLID", "NAME")

    val linkFinalColumns: List[String] = List(sidName, "FECHA_REGISTRO_DV", "ORIGEN_REGISTRO_DV",
      "DATOS_HOTBILLING_SID", "CELL_SID", "CLAVE_PARTICION")

    val conditions: Column = dfHB("cell_id") <=> dfCell("name") && dfHB("cell_id") <=> dfCell("name")

    StagingCore.createLink(spark, ResultPath, dfHB, dfCell, conditions, sidName, linkTableName, BDSShecma,
      keyColumns, linkFinalColumns, "inner")
  }
}
