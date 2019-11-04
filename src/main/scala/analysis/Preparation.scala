package analysis

import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.DataFrame

object Preparation {
  def stringIndexerData(src: DataFrame): StringIndexerModel = {

    //TODO NEED TO ADD PIPELINE !!

    val indexer = new StringIndexer()
      .setInputCol("os").setOutputCol("osIndexer")
      .setInputCol("network").setOutputCol("networkIndexer")
      .setInputCol("appOrSite").setOutputCol("appOrSiteIndexer")
      .setInputCol("interests").setOutputCol("interestsIndexer")

    val testModel = indexer.fit(src)
    testModel
  }
}
