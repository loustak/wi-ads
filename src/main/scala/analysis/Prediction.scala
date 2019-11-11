package analysis

import clean.Cleaner._
import clean.DataCleansing._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Prediction {
  def prediction(sparkSession: SparkSession, data: String): Unit = {
    val rawData = sparkSession.read.format("json")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(data)
      .withColumn("id", monotonically_increasing_id)
      .drop("label")
      .drop("_corrupt_record")

    import sparkSession.implicits._

    //Keep only columns that we need for ML
    val selectedData = rawData.select("os", "network", "appOrSite", "timestamp", "bidfloor", "size", "interests", "id", "label")

    val cleanedData = cleanData(selectedData, sparkSession)

    val model = PipelineModel.load("models/NBC")
    val predictions = model
      .transform(cleanedData)
      .withColumn("label", when($"prediction" === 0.0, false).otherwise(true))

    val result = predictions.select("label", "id")

    rawData.select("size").distinct().show()
    val dataSizeCleaned = cleanSizeColumn(rawData, sparkSession)

    val resultJoin = dataSizeCleaned.join(result, "id")

    val reorderedColumnNames: Array[String] = Array("label") ++ rawData.columns
    val resultFinal: DataFrame = resultJoin.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*).drop("id")

    resultFinal.write.mode("overwrite").option("header", "true").csv("result")

    println("[TheIllusionists] The CSV file is saved in the result folder.")
  }
}
