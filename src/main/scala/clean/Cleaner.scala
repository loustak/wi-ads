

import org.apache.spark.sql.SparkSession

import dataCleansing._

object Cleaner extends App {

  val context= SparkSession
    .builder
    .appName("the Illusionists")
    .master("local[*]")
    .getOrCreate()

  context.sparkContext.setLogLevel("WARN")


  val raw_data =  context.read.format("json")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("sample-1000.json")

  //Keep only columns that we need for ML
  val selected_data = raw_data.select("network", "appOrSite",  "timestamp","bidfloor","size", "interests","label")


  // Cleaning OS column
  val dataWithOsCleaned = cleanOsColumn(selected_data)
  println("dataset ::::::::"+ dataWithOsCleaned.show())
  context.close()



}






