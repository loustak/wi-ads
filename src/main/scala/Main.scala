import java.io.File

import analysis.Train
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import analysis.Train._

object Main extends App {
  override def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("The Illusionists")
      .getOrCreate()

    Console.println("[TheIllusionists] Application for prediction on RTB data.")

    /*
    if (args.length == 0)
      Console.println("[TheIllusionists] You should give the path to the json data file.")
    else {
      if (new File(args(0)).exists() && new File(args(0)).isFile && args(0).endsWith(".json"))
        prediction(spark, args(0))
      else
        Console.println("[TheIllusionists] The file is not valid !")
    }
     */

    trainModel(spark, "data/sample-10000.json")
    //prediction(spark, "data/sample-10000.json")
    spark.close()
  }
}