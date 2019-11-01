package calculs

import clean.DataCleansing._
import org.apache.spark.sql.SparkSession

object Calculs {

  def main(args: Array[String]): Unit = {
    val dataPath = "data"
    val data = dataPath + "/data-students.json"

    val spark = SparkSession.builder()
      // Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]"
      // to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
      .master("local[4]")
      // Sets a name for the application, which will be shown in the Spark web UI.
      .appName("ADS click prediction") // The app name in the web UI
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rtbData = spark.read.json(data)
    rtbData.printSchema()

    // Toutes les lignes des gens qui ont cliqué
    val trueClickRaw = rtbData.filter(rtbData("label") === true)
    trueClickRaw.show()

    val dataCleanOs = cleanOsColumn(trueClickRaw)
    val trueClick = cleanTimestampColumn(dataCleanOs)

    val allData = cleanTimestampColumn(rtbData)

    // Calcul du pourcentage de click
    val cr: Float = (trueClick.count().toFloat / rtbData.count().toFloat)
    println("CR: " + (cr * 100) + "%")

    // Est ce que appOrSite a une influence sur le click ?
    println("\nClick in function of appOrSite column: ")
    trueClick.groupBy("appOrSite").count().foreach(x => println(x(0) + " = " + x(1) + " clicks"))

    // Est ce que media a une influence sur le click ?
    println("\nClick in function of media column: ")
    trueClick.groupBy("media").count().foreach(x => println(x(0) + " = " + x(1) + " clicks"))

    // Est ce que os a une influence sur le click ?
    println("\nClick in function of os column: ")
    trueClick.groupBy("os").count().foreach(x => println(x(0) + " = " + x(1) + " clicks"))

    // Est ce que size a une influence sur le click ?
    println("\nClick in function of size column: ")
    trueClick.groupBy("size").count().foreach(x => println(x(0) + " = " + x(1) + " clicks"))

    // Est ce que network a une influence sur le click ?
    println("\nClick in function of network column: ")
    trueClick.groupBy("network").count().foreach(x => println(x(0) + " = " + x(1) + " clicks"))

    // Est ce que type a une influence sur le click ?
    println("\nClick in function of type column: ")
    trueClick.groupBy("type").count().foreach(x => println(x(0) + " = " + x(1) + " clicks"))

    // Est ce que exchange a une influence sur le click ?
    println("\nClick in function of exchange column: ")
    trueClick.groupBy("exchange").count().foreach(x => println(x(0) + " = " + x(1) + " clicks"))

    // Est ce que publisher a une influence sur le click ?
    println("\nClick in function of publisher column: ")
    trueClick.groupBy("publisher").count().foreach(x => println(x(0) + " = " + x(1) + " clicks"))

    // Est ce que timestamp a une influence sur le click ?
    println("\nClick in function of timestamp column: ")
    val nbPubTime = allData.groupBy("timestamp").count().as("totalpub")
    val nbTrueClickTime = trueClick.groupBy("timestamp").count()
    val fuseTime = nbTrueClickTime.join(nbPubTime, "timestamp")
    fuseTime.foreach(x => println(x(0) + "h = " + x(1) + " ads for a total of " + x(2) + " displayed."))

    /*rtbData.select("appOrSite").distinct().show()
    rtbData.select("bidfloor").distinct().show()
    rtbData.select("city").distinct().show() // drop
    rtbData.select("exchange").distinct().show()
    rtbData.select("interests").distinct().show()
    rtbData.select("media").distinct().show()
    rtbData.select("network").distinct().show()
    rtbData.select("os").distinct().show()
    rtbData.select("publisher").distinct().show()
    rtbData.select("size").distinct().show()
    rtbData.select("timestamp").distinct().show()
    rtbData.select("type").distinct().show()
    rtbData.select("user").distinct().show()*/

  }

}
