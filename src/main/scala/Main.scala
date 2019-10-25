import org.apache.spark.sql.SparkSession

object Main {

    def main(args: Array[String]): Unit = {

        val newline = System.lineSeparator()
        val dataPath = "data"
        val rddPath = "rdd"
        val baseDataPath = dataPath + "/data-students.json"
        val validJsonRddPath = rddPath + "/valid-json"

        val spark = SparkSession.builder()
            // Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]"
            // to run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
            .master("local[1]")
            // Sets a name for the application, which will be shown in the Spark web UI.
            .appName("ADS click prediction") // The app name in the web UI
            .getOrCreate()

        val sc = spark.sparkContext

        // Clean the file to produce valid JSON
        val text = sc.textFile(baseDataPath).zipWithIndex()

        val count = text.count()

        val textWithComma = text
            .map{ case (line, i) =>
                if (i == 0) {
                    "[" + newline + line + ","
                } else if (i == count - 1) {
                    line + newline + "]"
                } else {
                    line + ","
                }
            }

        textWithComma.saveAsTextFile(validJsonRddPath)

        spark.stop()
    }
}
