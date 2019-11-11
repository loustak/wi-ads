package clean

import java.text.SimpleDateFormat

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

import org.apache.spark.sql.functions._

object DataCleansing {

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
    //val treshold = 100000
    val treshold = 100

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

  def cleanNetworkColumn(src: DataFrame): DataFrame = {

    src.groupBy("network").count().show
    val mostOccuringValue = getMostOccuringValue(src,"network")
    val newSrc = src.withColumn("network",
      when(src.col("network").isNull,mostOccuringValue)
        .when(src.col("network") === "",mostOccuringValue)
        .otherwise(src.col("network")))

    val datawithNetworkCleaned = newSrc.withColumn("network", udfCleanNetworkColumn(newSrc("network")))
    val newDF = tolowerCase(datawithNetworkCleaned, "network")
    newDF
  }

  def getMostOccuringValue(src: DataFrame, column: String): String = {
    src.groupBy(column).count().orderBy(desc("count"))
      .select(column).filter(x => x(0) != null)
      .first()(0)
      .toString
  }



  def getMNCbyCode (code: String): String = {

    // France Mobile Country Code
    val MCCFrance = "208-"

    // France main Mobile Network Code
    val MNCFrance = Map("orange"->List("1","01","2","02","91","95"),
      "sfr"->List("9","09","10","11","13"),
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


  def udfCleanNetworkColumn: UserDefinedFunction = {
    udf {(row: String) =>
      getMNCbyCode(row)
    }
  }


  // ----------- Cleaning timestamp functions ------------- //

  def cleanTimestampColumn(src: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
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

  def cleanInterestsColumn(src: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val df = src.withColumn("interests", when(isnull($"interests"), "").otherwise($"interests"))
    val interestsWithoutLabel = df.withColumn("interests", split($"interests", ",").cast("array<String>"))
    val interests_clean_udf = udf(generalInterests _)
    val interestsCleaned = interestsWithoutLabel.withColumn("interests", interests_clean_udf($"interests"))
    createInterestsColumn(interestsCleaned, sparkSession)
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
  def createInterestsColumn(src: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
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
    newSrc.drop("interests")
  }

  // ----------- Cleaning size functions ------------- //

  def cleanSizeColumn(src: DataFrame, sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val sizes = src.select("size").distinct().rdd.map(r => r(0)).collect().toList
    val newSrc = src.withColumn("size", udf_size(sizes.asInstanceOf[List[mutable.WrappedArray[Long]]])($"size"))
    newSrc
  }

  def udf_size(sizes: List[mutable.WrappedArray[Long]]): UserDefinedFunction = {
    udf((row: mutable.WrappedArray[Long]) => {
      replaceSize(row, sizes)
    })
  }

  def replaceSize(row: mutable.WrappedArray[Long], sizes: List[mutable.WrappedArray[Long]]): String = {
    if (row != null) {
      if (sizes.contains(row)) {
        row(0) + "x" + row(1)
      } else {
        "null"
      }
    } else {
      "null"
    }
  }

  // ----------- Cleaning type functions ------------- //

  def cleanTypeColumn(src: DataFrame): DataFrame = {

    val mostOccuringValue = getMostOccuringValue(src,"type")
    val newSrc = src.withColumn("type",
      when(src.col("network").isNull,mostOccuringValue)
        .when(src.col("network") === "CLICK",mostOccuringValue)
        .otherwise(src.col("type")))

    println(mostOccuringValue)
    println(newSrc)
    newSrc
  }

  // ----------- Cleaning BidFloor functions ------------- //
  def cleanBidFloorColumn(src: DataFrame): DataFrame = {
    val avgBidFloor = src.select(mean("bidfloor")).first()(0).asInstanceOf[Double]
    println("avgbidfloor: " + avgBidFloor)
    val newSrc = src.withColumn("bidfloor",
      when(src.col("bidfloor").isNull,avgBidFloor)
        .otherwise(src.col("bidfloor"))
    )
    newSrc
  }

  def labelColumnToInt(src: DataFrame): DataFrame = {
    val newSrc = src.withColumn("label",
      when(src.col("label").isNull, 0)
        .when(src.col("label") === true, 1)
        .otherwise(0))
    newSrc
  }

  def putWeightsOnColumn(src: DataFrame): DataFrame = {
    val ratio = 0.97
    src.withColumn("weights", when(src.col("label").contains(1), ratio).otherwise(1 - ratio))
  }

  def addLabelColumn(src: DataFrame): DataFrame = {
    val getNullInt = udf(() => None: Option[Int])
    if(!src.columns.contains("label")) src.withColumn("label", getNullInt())
    else src
  }
}
