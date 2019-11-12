package clean

import java.text.SimpleDateFormat

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

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

    val resultSrc = removeColumns(newSrc, newSrc.columns.filter(x => !allIAB.contains(x) && x.startsWith("IAB")))
    addMissingIAB(resultSrc, allIAB.toArray)
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
      when(src.col("type").isNull,mostOccuringValue)
        .when(src.col("type") === "CLICK",mostOccuringValue)
        .otherwise(src.col("type")))
    newSrc
  }

  // ----------- Cleaning BidFloor functions ------------- //
  def cleanBidFloorColumn(src: DataFrame): DataFrame = {
    val avgBidFloor = src.select(mean("bidfloor")).first()(0).asInstanceOf[Double]
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
    val ratio = 0.9885
    src.withColumn("weights", when(src.col("label").contains(1), ratio).otherwise(1 - ratio))
  }

  def addLabelColumn(src: DataFrame): DataFrame = {
    val getNullInt = udf(() => None: Option[Int])
    if(!src.columns.contains("label")) src.withColumn("label", getNullInt())
    else src
  }

  def removeColumns(src: DataFrame, columns: Array[String]): DataFrame = {
    var newSrc = src
    for (c <- columns) {
      newSrc = newSrc.drop(newSrc.col(c))
    }
    newSrc
  }

  def addMissingIAB(src: DataFrame, columns: Array[String]): DataFrame = {
    columns.foldLeft(src)((dataframe, column) => {
      if (!dataframe.columns.contains(column)) dataframe.withColumn(column, lit(0))
      else dataframe
    }
    )
  }

  val allIAB =
    List("IAB2-15", "IAB19-4", "IAB9-26", "IAB9-18", "IAB8-8", "IAB6-5", "IAB11-4", "IAB9-29", "IAB9", "IAB15-6", "IAB2-4", "IAB17-41", "IAB8-7", "IAB13-3", "IAB9-1", "IAB17-30", "IAB24", "IAB7-39", "IAB9-8", "IAB2-5", "IAB9-22", "IAB4-5", "IAB6", "IAB9-5", "IAB17-11", "IAB17-17", "IAB9-12", "IAB1-5", "IAB19", "IAB19-16", "IAB9-11", "IAB17-4", "IAB13", "IAB9-23", "IAB9-14", "IAB17-28", "IAB17-37", "IAB4", "IAB14-7", "IAB17-1", "IAB7-44", "IAB9-25", "IAB6-2", "IAB10-2", "IAB1-7", "IAB11-2", "IAB17", "IAB15-1", "IAB8", "IAB17-35", "IAB14-3", "IAB9-30", "IAB18-5", "IAB5", "IAB23-4", "IAB1-1", "IAB17-9", "IAB12", "IAB7-19", "IAB17-29", "IAB9-17", "IAB2-22", "IAB23", "IAB17-36", "IAB9-24", "IAB7-34", "IAB1", "IAB20", "IAB1-6", "IAB9-13", "IAB2-1", "IAB16", "IAB19-28", "IAB9-4", "IAB18-6", "IAB7-45", "IAB19-24", "IAB7-30", "IAB9-20", "IAB15", "IAB3", "IAB17-8", "IAB22", "IAB7-37", "IAB15-4", "IAB2-17", "IAB17-25", "IAB11", "IAB19-6", "IAB8-5", "IAB9-28", "IAB12-3", "IAB2-21", "IAB3-12", "IAB16-5", "IAB26", "IAB19-1", "IAB6-7", "IAB1-2", "IAB9-27", "IAB12-2", "IAB13-9", "IAB17-26", "IAB9-16", "IAB18-3", "IAB17-15", "IAB17-39", "IAB9-31", "IAB20-18", "IAB19-18", "IAB14-6", "IAB17-2", "IAB19-29", "IAB9-3", "IAB2", "IAB4-9", "IAB2-3", "IAB7-31", "IAB14", "IAB9-19", "IAB21", "IAB19-2", "IAB19-5", "IAB15-10", "IAB17-12", "IAB18-1", "IAB19-14", "IAB10", "IAB16-3", "IAB9-2", "IAB25", "IAB7-36", "IAB7", "IAB10-7", "IAB9-6", "IAB17-27", "IAB9-7", "IAB18-4", "IAB8-9", "IAB3-11", "IAB9-21", "IAB17-40", "IAB14-1", "IAB1-4", "IAB9-10", "IAB7-1", "IAB12-1", "IAB22-3", "IAB18", "IAB9-15", "IAB3-1", "IAB7-32", "IAB17-16", "IAB17-38", "IAB17-44", "IAB17-3", "IAB15-8")

}
