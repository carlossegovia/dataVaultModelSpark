package core

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object Utils {

  /**
    * UDF for transform date with the format "yyyy-MM-dd HH:mm:ss.SSSZ" to "yyyyMMdd"
    */
  val convertDateUDF: UserDefinedFunction = udf {
    date: String => date.split(" ")(0).replace("-", "")
  }

  /**
    * UDF for transform date with the format yyyy-MM-dd HH:mm:ss.SSSZ" to military time clock
    */
  val convertHourUDF: UserDefinedFunction = udf {
    date: String => date.split(" ")(1).split(":")(0).toInt * 10000
  }

  /**
    * UDF for transform data volumen to KB, MB or GB.
    */
  val convertBytesToKBUDF: UserDefinedFunction = udf {
    data: Long => (data / 1024.0).toFloat
  }

  /**
    * Drop duplicate columns from a Dataframe
    *
    * @param df : Spark dataframe
    * @return
    */
  def dropDuplicates(df: DataFrame): DataFrame = {
    var newCols = new ListBuffer[String]()
    var dupCols = new ListBuffer[String]()
    val columns = df.columns.map(_.toLowerCase())

    for (i <- columns.indices) {
      if (!newCols.contains(columns(i)))
        newCols += columns(i)
      else
        dupCols += i.toString
    }

    val columnsTemp = List.range(0, columns.length).map(_.toString)
    val dfTemp = df.toDF(columnsTemp: _*).drop(dupCols: _*)
    dfTemp.toDF(newCols: _*)
  }

  /**
    * Return the SQL query for create a external table
    *
    * @param dfSchema    : Spark dataframe schema
    * @param tableName   : Name of the Hive/Impala table
    * @param tablePath   : HDFS path for store the table
    * @param tableSchema : Schema of the Hive/Impala table
    * @return
    */
  def createTableSentence(dfSchema: StructType, tableName: String, tablePath: String, tableSchema: String = "default")
  : String = {
    val columns = dfSchema.map(col => s"${col.name} ${col.dataType.simpleString}").mkString(", ")
    s"CREATE EXTERNAL TABLE IF NOT EXISTS $tableSchema.$tableName ($columns) STORED AS PARQUET LOCATION " +
      s"'$tablePath'"
  }

  /**
    * Store a Spark dataframe as a Hive/Impala external table
    *
    * @param spark       : Spark Session
    * @param df          : Spark dataframe
    * @param tableName   : Name of the Hive/Impala table
    * @param tableSchema : Schema of the Hive/Impala table
    * @param resultPath  : HDFS path for store the table
    * @param saveMode    : By default overwrite the data. Another possible value is 'append'
    */
  def storeDf(spark: SparkSession, df: DataFrame, tableName: String, tableSchema: String, resultPath: String,
              saveMode: String = "overwrite"): Unit = {

    spark.sql(s"DROP TABLE IF EXISTS $tableSchema.$tableName")
    df.write.mode(saveMode).option("compression", "gzip").parquet(resultPath)
    val sqlSentence = createTableSentence(df.schema, tableName, resultPath, tableSchema)
    spark.sql(sqlSentence)
    println(sqlSentence)
  }
}
