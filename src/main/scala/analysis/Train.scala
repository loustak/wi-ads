package analysis

import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object Train {
  def logisticReg(train: DataFrame): PipelineModel = {

    val osIndexer = new StringIndexer().setInputCol("os").setOutputCol("osIndexer")
    val networkIndexer = new StringIndexer().setInputCol("network").setOutputCol("networkIndexer")
    val appOrSiteIndexer = new StringIndexer().setInputCol("appOrSite").setOutputCol("appOrSiteIndexer")
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("labelIndexer")
    val publisherIndexer = new StringIndexer().setInputCol("publisher").setOutputCol("publisherIndexer")
    val cityIndexer = new StringIndexer().setInputCol("city").setOutputCol("cityIndexer")
    val mediaIndexer = new StringIndexer().setInputCol("media").setOutputCol("mediaIndexer")
    val sizeIndexer = new StringIndexer().setInputCol("size").setOutputCol("sizeIndexer")
    val typeIndexer = new StringIndexer().setInputCol("type").setOutputCol("typeIndexer")

    val allColumns = Array("osIndexer","networkIndexer","appOrSiteIndexer","labelIndexer","publisherIndexer","cityIndexer","mediaIndexer","sizeIndexer","typeIndexer")

    val assembler = new VectorAssembler()
      .setInputCols(allColumns)
      .setOutputCol("rawFeatures")

    //vector slicer
    val slicer = new VectorSlicer().setInputCol("rawFeatures").setOutputCol("slicedFeatures").setNames(allColumns)

    //scale the features
    val scaler = new StandardScaler().setInputCol("slicedFeatures").setOutputCol("features").setWithStd(true).setWithMean(true)

    //label for binaryClassifier
    //val binarizerClassifier = new Binarizer().setInputCol("label").setOutputCol("binaryLabel")

    //logistic regression
    val logisticRegression = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setLabelCol("binaryLabel").setFeaturesCol("features")

    //Chain indexers and tree in a Pipeline
    val lrPipeline = new Pipeline().setStages(Array(osIndexer,networkIndexer,appOrSiteIndexer,labelIndexer,publisherIndexer,cityIndexer,mediaIndexer,sizeIndexer,typeIndexer,assembler,slicer,scaler,logisticRegression))

    // Train model
    val lrModel = lrPipeline.fit(train)

    lrModel
  }

  def decisionTree(train: DataFrame): PipelineModel = {

    val osIndexer = new StringIndexer().setInputCol("os").setOutputCol("osIndexer")
    val networkIndexer = new StringIndexer().setInputCol("network").setOutputCol("networkIndexer")
    val appOrSiteIndexer = new StringIndexer().setInputCol("appOrSite").setOutputCol("appOrSiteIndexer")
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("labelIndexer")
    val publisherIndexer = new StringIndexer().setInputCol("publisher").setOutputCol("publisherIndexer")
    val cityIndexer = new StringIndexer().setInputCol("city").setOutputCol("cityIndexer")
    val mediaIndexer = new StringIndexer().setInputCol("media").setOutputCol("mediaIndexer")
    val sizeIndexer = new StringIndexer().setInputCol("size").setOutputCol("sizeIndexer")
    val typeIndexer = new StringIndexer().setInputCol("type").setOutputCol("typeIndexer")

    val allColumns = Array("osIndexer","networkIndexer","appOrSiteIndexer","labelIndexer","publisherIndexer","cityIndexer","mediaIndexer","sizeIndexer","typeIndexer")

    val assembler = new VectorAssembler()
      .setInputCols(allColumns)
      .setOutputCol("rawFeatures")

    //index category index in raw feature
    val indexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("rawFeaturesIndexed").setMaxCategories(10)
    //PCA
    val pca = new PCA().setInputCol("rawFeaturesIndexed").setOutputCol("features").setK(10)
    //label for multi class classifier
    //val bucketizer = new Bucketizer().setInputCol("label").setOutputCol("multiClassLabel").setSplits(Array(Double.NegativeInfinity, 0.0, 15.0, Double.PositiveInfinity))

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")
    // Chain all into a Pipeline
    val dtPipeline = new Pipeline().setStages(Array(osIndexer, networkIndexer, appOrSiteIndexer,labelIndexer,publisherIndexer,cityIndexer,mediaIndexer,sizeIndexer,typeIndexer, assembler, indexer, pca, bucketizer, dt))
    // Train model.
    val dtModel = dtPipeline.fit(train)

    dtModel
  }


}
