package core

import org.apache.spark.sql.DataFrame

object EnterpriseCore {

  val ResultPath = "/datavault/enterprise/"
  val BDESchema = "BDE"
  val allowedTransformations: List[String] = List[String]("transformBytesToKBytes")

  /**
    * Create an Hub or Sat table for the enterprise stage.
    *
    * @param df              : Spark dataframe
    * @param tableName       : Name for the table
    * @param transformations : List of transformations over some columns in the dataframe
    */
  def createHubSat(df: DataFrame, tableName: String, transformations: List[List[String]]): Unit = {

    // TODO -> must be allowed some transformations over some columns in the df

    df.write.mode("append").option("compression", "gzip")
      .parquet(ResultPath + tableName.replace("BDS", "BDE"))

    println(s"INSERT INTO ENTERPRISE TABLE $BDESchema${tableName.replace("BDS", "BDE")}")
  }

  /**
    * Create a Link table for the enterprise stage
    *
    * @param df
    * @param tableName
    */
  def createLink(df: DataFrame, tableName: String): Unit = {
    df.write.mode("append").option("compression", "gzip")
      .parquet(ResultPath + tableName.replace("BDS", "BDE"))

    println(s"INSERT INTO ENTERPRISE TABLE $BDESchema${tableName.replace("BDS", "BDE")}")

  }
}
