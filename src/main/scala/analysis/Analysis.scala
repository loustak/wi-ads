package analysis

import analysis.Models._
import clean.DataCleansing._
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Analysis {
  def analyse(src: DataFrame): Unit = {

    val splits = src.randomSplit(Array(0.8, 0.2), seed = 11L)
    val trainDF = putWeightsOnColumn(src)
    val testDF = splits(1).cache()

    val cleanTestDF = addLabelColumn(testDF)

    val model = logiscticTest(trainDF)
    //val model = naiveBayesClassifier(trainDF)

    // Make predictions
    val lrPredictions = model.transform(cleanTestDF)

    val result = lrPredictions.select("prediction", "label")

    val predictionAndLabels = result.rdd.map { row =>
      (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Int].toDouble)
    }

    printBinaryMetrics(predictionAndLabels)
    printConfusionMatrix(predictionAndLabels)

  }

  def printConfusionMatrix(predictionAndLabels: RDD[(Double, Double)]): Unit = {
    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(s"ConfusionMatrix:\n ${metrics.confusionMatrix.toString()}")
    println(s"Precision: ${metrics.precision}")
    println(s"weighted Precision: ${metrics.weightedPrecision}")
    println(s"Recall: ${metrics.recall}")
    println(s"weighted Recall: ${metrics.weightedRecall}")
    println(s"FMeasure: ${metrics.fMeasure}")
    println(s"weighted FMeasure: ${metrics.weightedFMeasure}")
    println(s"${metrics.truePositiveRate(metrics.labels(0))}")
  }

  def printBinaryMetrics(predictionAndLabels: RDD[(Double,Double)]): Unit = {
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    //Area under ROC
    println(s"Area under ROC = ${metrics.areaUnderROC()}")


    //Area under percision-recall curve
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
    println(s"Threshold: $t, Precision: $p")
  }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
    println(s"Threshold: $t, Recall: $r")
  }

    // F-measure with beta 1
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
    println(s"Threshold: $t, F-score: $f, Beta = 1")
  }

    // Recall: β is chosen such that recall is considered β times as important as precision

    // F-measure with beta 0.5
    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    fScore.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }
  }
}
