package clean

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lower, udf, when}


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

    // => 300 for the sample-1000 else for the data-students.json set it to 100000
    val treshold = 300

    val counts = src.groupBy(columnName).count()
    val joined = src.join(counts, Seq("os"))
    joined.withColumn("os", when(joined("count") >= treshold, joined("os")).otherwise("other"))
  }

  def cleanOsColumn(src: DataFrame):DataFrame = {

    val srclowerCase = DataCleansing.tolowerCase(src, "os")
    val srcWindows = DataCleansing.windowsToOneLabel(srclowerCase)
    DataCleansing.mainLabelsVSother(srcWindows, "os")

  }

  // ----------- Cleaning interests functions ------------- //

  //TODO
  def replace_InterestRegex(src: DataFrame, interests:List[String]):DataFrame = ???

  /*
      val regex = new Regex("IAB-(.*)");
      val interestsList = interests.split(',').map(interest => regex.replace.....)
  */

  //TODO
  def cleanInterestsColumn(src: DataFrame): DataFrame = ???

  //val interests = List("IAB1","IAB2","IAB3","IAB4","IAB5","IAB6","IAB7","IAB8","IAB9","IAB10","IAB11","IAB12","IAB13","IAB14","IAB15","IAB16","IAB17","IAB18","IAB19","IAB20","IAB21")
  //val l= src.withColumn("interests", when("interests", ))
    // First step = Remplacer les codes détaillés par des codes plus généralistes
    // Second step = Splitter les lists de plusieurs interests => explode function
    // Third step = For int


  // ----------- Cleaning columns functions ------------- //

  //TODO

  def cleanNetworkColumn(src: DataFrame):DataFrame = ???

  def cleanTimestampColumn(src: DataFrame): DataFrame = {
    // To apply a function to each value of a column
    val timestamp_clean_udf = udf(timeStampToDate _)
    val newSrc = src.withColumn("timestamp", timestamp_clean_udf($"timestamp"))
    newSrc
  }

  def cleanSizeColumn(src: DataFrame):DataFrame = ???

  def cleanBidFloorColumn(src: DataFrame):DataFrame = ???

  def cleanTypeColumn(src: DataFrame):DataFrame = ???

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

}
