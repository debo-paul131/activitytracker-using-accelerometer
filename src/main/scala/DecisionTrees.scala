

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel

object DecisionTrees {

  def createModel(sc: SparkContext, trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Double = {
    val categoricalFeaturesInfo: Map[Int, Int] = Map()
    val numClasses = 6
    val impurity = "gini"
    val maxDepth = 9
    val maxBins = 32

    // create model
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    model.save(sc, "actitracker")
    // Evaluate model on training instances and compute training error
    val predictionAndLabel: RDD[(Double, Double)] = testData.map {
      data => (model.predict(data.features), data.label)
    }

    val testErrDT = 1.0 * predictionAndLabel.filter(pl => !pl._1.equals(pl._2)).count() / testData.count()
    return testErrDT
  }
}