package clean

import clean.DataCleansing._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Cleaner {
    def cleanData(data: DataFrame, sparkSession: SparkSession): DataFrame = {
        /*
        println("av os: " + data.count())

        // Cleaning OS column
        val dataWithOsCleaned = cleanOsColumn(data)

        println("av time: " + dataWithOsCleaned.count())

        // Cleaning Timestamp column
        val dataWithTimestampCleaned = cleanTimestampColumn(dataWithOsCleaned, sparkSession)

        println("av network: " + dataWithTimestampCleaned.count())

        // Cleaning Network column
        val dataWithNetworkCleaned = cleanNetworkColumn(dataWithTimestampCleaned)
        */
        println("av interests: " + data.count())

        // Cleaning Interests column
        val dataWithInterestsCleaned = cleanInterestsColumn(data, sparkSession)

        /*
        println("av BidFloorColumn: " + dataWithInterestsCleaned.count())

        // Cleaning BidFloorColumn
        var dataWithBidFloorCleaned = cleanBidFloorColumn(dataWithInterestsCleaned)

        if (!data.columns.toList.contains("label"))
            dataWithBidFloorCleaned = addLabelColumn(dataWithBidFloorCleaned)

        println("av dataWithLabelCleaned: " + dataWithBidFloorCleaned.count())

         */

        val dataWithLabelCleaned = labelColumnToInt(dataWithInterestsCleaned)

        println("av dataWithTypeCleaned: " + dataWithLabelCleaned.count())
        val dataWithTypeCleaned = cleanTypeColumn(dataWithLabelCleaned)

        println("av dataWithSizeCleaned: " + dataWithLabelCleaned.count())
        cleanSizeColumn(dataWithTypeCleaned, sparkSession)
    }
}






