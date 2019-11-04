import java.io.File

import org.apache.spark.sql.{SQLContext, SparkSession}

object Main {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
            // Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]"
            // to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
            .master("local[1]")
            // Sets a name for the application, which will be shown in the Spark web UI.
            .appName("ADS click prediction") // The app name in the web UI
            .getOrCreate()

        val sc = spark.sparkContext

        val dataFrame = spark.read.json(Path.baseDataPath)
        dataFrame.show(5)

        sc.stop()
        spark.stop()
    }
}
