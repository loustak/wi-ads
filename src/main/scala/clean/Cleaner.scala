package clean

import analysis.Analysis
import clean.DataCleansing._
import org.apache.spark.sql.SparkSession

object Cleaner extends App {

  val dataPath = "data/"
  val data = dataPath + "sample-1000.json"

  val context= SparkSession
    .builder
    .appName("the Illusionists")
    .master("local[4]")
    .getOrCreate()

  context.sparkContext.setLogLevel("WARN")

  val raw_data =  context.read.format("json")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(data)

  //Keep only columns that we need for ML
  val selected_data = raw_data.select("os", "network", "appOrSite", "timestamp", "bidfloor", "size", "interests", "label")
  val cleanDF = clean_data(selected_data)


  println("dataset before cleaning:")
  //selected_data.show()

  println("dataset after cleaning:")
  //cleanDF.show()




  Analysis.analyse(cleanDF)

  context.close()
}






