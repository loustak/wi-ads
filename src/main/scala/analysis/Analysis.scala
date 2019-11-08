package analysis

import analysis.Train._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.DataFrame

object Analysis {
  def analyse(src: DataFrame): Unit = {

    val splits = src.randomSplit(Array(0.8,0.2),seed = 11L )
    val train = splits(0).cache()
    val test = splits(1).cache()
    val model = logisticReg(train)


    // Make predictions
    val lrPredictions = model.transform(test)

    lrPredictions.select("prediction","label","features").show(20)

    val result = lrPredictions.select("prediction","label")

    val predictionAndLabels = result.rdd.map { row =>
      (row.get(0).asInstanceOf[Double],row.get(1).asInstanceOf[Double])
    }

    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    //Area under ROC
    println(s"Area under ROC = ${metrics.areaUnderROC()}")

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


    //Set the model threshold to maximize F-Measure
    //val fMeasure = metrics.fMeasureByThreshold
    //val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    //val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).
    //  select("threshold").head().getDouble(0)
    //model.setThreshold(bestThreshold)
  }
}
