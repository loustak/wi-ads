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

    //Keep only columns that we need for ML
    val selectedData = rawData.select("os", "network", "appOrSite", "timestamp", "bidfloor", "size", "interests", "label")
    val cleanedData = cleanData(selectedData, sparkSession)
    analyse(cleanedData)
  }

}
