// Databricks notebook source
// MAGIC %md
// MAGIC #Delta table tranfer validation
// MAGIC
// MAGIC This notebook checks the copy of a delta table from one container to another.\
// MAGIC It validates 3 kpis:
// MAGIC * The total number of rows
// MAGIC * The number of rows group by specific column
// MAGIC * the delta table history
// MAGIC
// MAGIC ```
// MAGIC Note for the history comparison:
// MAGIC -------------------------------
// MAGIC
// MAGIC The timestamp is the the last modification of the corresponding metadat file on the system.
// MAGIC As azcopy is not able to preserve this value it is excluded from the comparison.
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC ##Global parameters
// MAGIC ---

// COMMAND ----------

val defaultSourceStorageAccount = "srcstorageaccount"
val defaultDestinationStorageAccount = "dststorageaccount"

// COMMAND ----------

// MAGIC %md
// MAGIC ##Define utility functions
// MAGIC ---

// COMMAND ----------


import org.apache.spark.sql.{DataFrame,Column}
import org.apache.spark.sql.functions._
import io.delta.tables._

/**
 * Constructs a path string using the provided parameters.
 *
 * @param container The name of the container.
 * @param storageAccount The
 * @return A formatted string that combines these parameters into a path.
 */
def createPath(container: String, path: String, storageAccount: String): String = s"abfss://${container}@${storageAccount}.dfs.core.windows.net/${path}"

/**
 * Container validation parameters
 *
 * @param sourceContainer The name of the source container.
 * @param destinationContainer The name of the destination container.
 * @param sourceDirectory The origin path of the dataset.
 * @param destinationDirectory The destination path of the dataset, None by default will use origin path.
 * @param sourceStorageAccount
 * @param destinationStorageAccount
 * @param groupColumn the column used to group data for detailed check.
 */
case class ContainerValidationParam(
                                     sourceContainer: String,
                                     destinationContainer: Option[String] = None,
                                     sourceDirectory: String,
                                     destinationDirectory: Option[String] = None,
                                     sourceStorageAccount: Option[String] = None,
                                     destinationStorageAccount: Option[String] = None,
                                     groupColumn: Column = $"date"
                                   ) {

  def sourcePath: String = createPath(container= sourceContainer, path= sourceDirectory, storageAccount= sourceStorageAccount.getOrElse(defaultSourceStorageAccount))

  def destinationPath: String = {
    val path = destinationDirectory match {
      case Some(destPath) => destPath
      case _ => sourceDirectory
    }

    val container = destinationContainer match {
      case Some(dstContainer) => dstContainer
      case _ => sourceContainer
    }

    createPath(container= container, path= path, storageAccount= destinationStorageAccount.getOrElse(defaultDestinationStorageAccount))
  }
}

// COMMAND ----------

/**
 * Check the delta table copy
 *
 * @param sourcePath the source path to the delta table
 * @param destinationPath the destination path to the delta table
 * @param groupCol the column the group on for the detail comparison
 *
 * @throws Exception in case of comparison failure
 */
def checkContainer(params: ContainerValidationParam): Boolean = {
  println("****************************************************************")
  println(s" Check ${params.sourcePath}")
  println("")

  val sourceDF =  spark.read.format("delta").load(params.sourcePath)
  val destinationDF = spark.read.format("delta").load(params.destinationPath)

  val checkTotal = checkTotalRows(sourceDF, destinationDF)
  val checkPerGroup = checkRowsPerGroup(sourceDF, destinationDF, params.groupColumn)
  val checkHistory = checkDeltaTableHistory(params.sourcePath, params.destinationPath)

  val success = checkTotal && checkPerGroup && checkHistory
  if (!success) {
    throw new Exception("Comparison failed")
  }

  return success
}

/**
 * Check the total number of rows
 *
 * @param sourceDF the source delta table
 * @param destinationDF the destination delta table
 * @return true if the number of rows matches
 */
def checkTotalRows(sourceDF: DataFrame, destinationDF: DataFrame): Boolean ={
  val sourceNbRows = sourceDF.count
  val destinationNbRows = destinationDF.count

  val success = sourceNbRows == destinationNbRows

  if(success) {
    println("checkTotalRows succeed!")
  } else {
    println("checkTotalRows failed...")
  }

  return success
}

/**
 * Check the number of rows per group
 *
 * @param sourceDF the source delta table
 * @param destinationDF the destination delta table
 * @param groupCol the column the group on for the detail comparison
 * @return true if the number of rows per group matches
 */
def checkRowsPerGroup(sourceDF: DataFrame, destinationDF: DataFrame, groupCol: Column): Boolean ={
  val sourceByDay = sourceDF.groupBy(groupCol).count.withColumnRenamed("count", "sourceCount")
  val destinationNbRows = destinationDF.groupBy(groupCol).count.withColumnRenamed("count", "destinationCount")

  val comparisonDF = sourceByDay.join(destinationNbRows, Seq(groupCol.toString()), "full_outer")
                                .withColumn("match", when($"destinationCount".isNotNull, $"sourceCount" === $"destinationCount").otherwise(false))
                                .filter(!$"match")

  if(!comparisonDF.isEmpty) {
    println("checkRowsPerGroup failed...")
    comparisonDF.show(false)
    return false
  } else {
    println("checkRowsPerGroup succeed!")
    return true
  }
}

/**
 * Transform the history dataset to convert map structure to array
 * @param dataframe the history dataframe
 * @return The formatted history dataframe
 */
def formatHistoryDataframe(dataframe: DataFrame): DataFrame = {

  dataframe.withColumn("operationMetricsKeys", map_keys($"operationMetrics"))
           .withColumn("operationMetricsValues", map_values($"operationMetrics"))
           .withColumn("operationMetrics", arrays_zip($"operationMetricsKeys", $"operationMetricsValues"))
           .withColumn("operationParametersKeys", map_keys($"operationParameters"))
           .withColumn("operationParametersValues", map_values($"operationParameters"))
           .withColumn("operationParameters", arrays_zip($"operationParametersKeys", $"operationParametersValues"))
           .drop("timestamp")
}

/**
 * Check the delta table history
 *
 * @param sourcePath the source path to the delta table
 * @param destinationPath the destination path to the delta table
 * @return true if the history matches
 */
def checkDeltaTableHistory(sourcePath: String, destinationPath: String): Boolean ={
  val source = DeltaTable.forPath(spark, sourcePath).history.toDF.transform(formatHistoryDataframe)
  val destination = DeltaTable.forPath(spark, destinationPath).history.toDF.transform(formatHistoryDataframe)

  val comparisonDF = source.except(destination)

  if(!comparisonDF.isEmpty) {
    println("checkDeltaTableHistory failed...")
    comparisonDF.show(false)
    return false
  } else {
    println("checkDeltaTableHistory succeed!")
    return true
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##Copy Validation
// MAGIC ---

// COMMAND ----------

// MAGIC %md
// MAGIC ###Define containers to check

// COMMAND ----------
val physicalPhase= "pdt"

val containersToCheck = Seq(
  ContainerValidationParam(sourceContainer= "exampleSourceContainer1", sourceDirectory= s"${physicalPhase}/exampleSourceDirectory1", destinationDirectory= Some("exampleDestinationDirectory1")),
  ContainerValidationParam(sourceContainer= "exampleSourceContainer2", sourceDirectory= s"${physicalPhase}/exampleSourceDirectory2", destinationDirectory= Some("exampleDestinationDirectory2")),
  ContainerValidationParam(sourceContainer= "exampleSourceContainer3", destinationContainer= Some("exampleDestinationContainer3"), sourceDirectory= "${physicalPhase}/exampleSourceDirectory3", destinationDirectory= Some("exampleDestinationDirectory3")),
)


// COMMAND ----------

// MAGIC %md
// MAGIC ###Check transfert

// COMMAND ----------

containersToCheck.foreach(checkContainer)