package analysis

import analysis.Analysis._
import clean.Cleaner.cleanData
import org.apache.spark.sql.SparkSession

object Train {

  def trainModel(sparkSession: SparkSession, data: String): Unit = {
    val rawData = sparkSession.read.format("json")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(data)

    println("NUMBER OF LINES BEFORE CLEAN: " + rawData.count())

    //Keep only columns that we need for ML
    val selectedData = rawData.select("os", "network", "appOrSite", "timestamp", "bidfloor", "size", "interests", "label", "type")
    val cleanedData = cleanData(selectedData, sparkSession)

    println("NUMBER OF LINES AFTER CLEAN: " + cleanedData.count())
    analyse(cleanedData)
  }

}
