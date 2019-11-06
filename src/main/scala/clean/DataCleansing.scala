package clean

import java.text.SimpleDateFormat

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable



object DataCleansing {

  val spark: SparkSession = SparkSession.builder()
    // Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]"
    // to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
    .master("local[4]")
    // Sets a name for the application, which will be shown in the Spark web UI.
    .appName("ADS click prediction") // The app name in the web UI
    .getOrCreate()

  import spark.implicits._ // To use the "$" keyword



  // ----------- Cleaning os functions ------------- //

  /**
   *
   * @param src dataFrame to modify
   * @param columnName column whose we want to convert values to lowercase
   * @return dataFrame with columnName's values in lowercase
   */
  def tolowerCase(src: DataFrame, columnName: String) : DataFrame = src.withColumn(columnName, lower(src.col(columnName)))


  // Set all different windows os denomination to one and only  "windows" name
  def windowsToOneLabel(src: DataFrame) : DataFrame = src.withColumn("os", when(src.col("os").contains("windows"), "windows").otherwise(src("os")))

  // Add a new category "other" for values where occurrence is less than the limit treshold
  def mainLabelsVSother(src: DataFrame, columnName: String) : DataFrame = {

    val treshold = 100000

    val counts = src.groupBy(columnName).count()
    val joined = src.join(counts, Seq(columnName))
    joined.withColumn(columnName, when(joined("count") >= treshold, joined(columnName)).otherwise("other")).drop("count")
  }

  def cleanOsColumn(src: DataFrame):DataFrame = {

    val srclowerCase = DataCleansing.tolowerCase(src, "os")
    val srcWindows = DataCleansing.windowsToOneLabel(srclowerCase)
    DataCleansing.mainLabelsVSother(srcWindows, "os")

  }


  // ----------- Cleaning network functions ------------- //


  def getMNCbyCode (code: String): String = {

    // France Mobile Country Code
    val MCCFrance = "208-"

    // France main Mobile Network Code
    val MNCFrance = Map("orange"->List("01","02","91","95"),
      "sfr"->List("09","10","11","13"),
      "bouygues"->List("20","21"),"free"->List("15","16"))

    if(code.startsWith(MCCFrance)) {
      val mnc = code.substring(4)
      val res = MNCFrance.map(m=> {
      val contained = m._2.contains(mnc)
      if (contained) m._1
        else ""
      }).mkString("")
      res
    }
    else "other"
  }


  def cleanNetworkColumn: UserDefinedFunction = {
    udf {(row: String) =>
      getMNCbyCode(row)
    }
  }


  // ----------- Cleaning timestamp functions ------------- //

  def cleanTimestampColumn(src: DataFrame): DataFrame = {
    // To apply a function to each value of a column
    val timestamp_clean_udf = udf(timeStampToDate _)
    src.withColumn("timestamp", timestamp_clean_udf($"timestamp"))
  }

  /**
   * To convert a timestamp to an Hour hh:mm:ss
   *
   * @param timestamp
   * @return
   */
  def timeStampToDate(timestamp: Long): String = {
    val ts = timestamp * 1000L
    val df = new SimpleDateFormat("HH:mm:ss")
    val date = df.format(ts)
    date.split(':')(0)
  }

  // ----------- Cleaning interests functions ------------- //

  def cleanInterestsColumn(src: DataFrame): DataFrame = {
    val df = src.where(col("interests").isNotNull)
    val interestsCleaned = df.withColumn("interests", split($"interests", ",").cast("array<String>"))
    val interests_clean_udf = udf(generalInterests _)
    interestsCleaned.withColumn("interests", interests_clean_udf($"interests"))
    createInterestsColumn(interestsCleaned)
  }

  def generalInterests(array: mutable.WrappedArray[String]): Array[String] = {
    var set = array.map(x => if (x.startsWith("IAB")) {
      x.toUpperCase()
    } else {
      ""
    }).toSet
    set -= ""
    set.toArray
  }

  /**
   * Function to create a column for each interests of a row
   *
   * @param src
   * @return
   */
  def createInterestsColumn(src: DataFrame): DataFrame = {
    val v = src.select("interests").rdd.map(r => r(0)).collect()
    var setInterests = Set[String]()
    var newSrc = src
    v.foreach(x => {
      x.asInstanceOf[mutable.WrappedArray[String]].foreach(r => {
        setInterests += r
      })
    })
    setInterests.foreach(x => {
      newSrc = newSrc.withColumn(x.toString, when(array_contains($"interests", x.toString), 1).otherwise(0))
    })
    newSrc
  }
}
