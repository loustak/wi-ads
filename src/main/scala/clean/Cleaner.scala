package clean

import clean.DataCleansing._
import org.apache.spark.sql.SparkSession

object Cleaner extends App {

  val dataPath = "data"
  val data = dataPath + "data-students.json"

  val context= SparkSession
    .builder
    .appName("the Illusionists")
    .master("local[*]")
    .getOrCreate()

  context.sparkContext.setLogLevel("WARN")

  val raw_data =  context.read.format("json")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(data)

  //Keep only columns that we need for ML
  val selected_data = raw_data.select("os", "network", "appOrSite", "timestamp", "bidfloor", "size", "interests", "label")


  // Cleaning OS column
  val dataWithOsCleaned = cleanOsColumn(selected_data)
  println("dataset ::::::::")
  dataWithOsCleaned.show()

  // Cleaning Timestamp column
  val dataWithTimestampCleaned = cleanTimestampColumn(dataWithOsCleaned)
  dataWithTimestampCleaned.show()

  // Cleaning Network column
  val datawithNetworkCleaned = dataWithTimestampCleaned.withColumn("network",cleanNetworkColumn(selected_data("network")))
  tolowerCase(datawithNetworkCleaned,"network")

  // Cleaning Interests
  val dataWithInterestsCleaned = cleanInterestsColumn(datawithNetworkCleaned)
  //dataWithInterestsCleaned.show()

  context.close()
}






