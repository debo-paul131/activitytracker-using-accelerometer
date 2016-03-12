
import org.apache.spark._

import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.sql.SQLContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

object ActivityTracker {
  case class AccelerometerData(userId: Int, activity: String, timestamp: Long, acc_x: Double, acc_y: Double, acc_z: Double)
  private val ACTIVITIES = List("Standing", "Jogging", "Walking", "Sitting", "Upstairs", "Downstairs");

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Activity Tracker")
      .set("spark.executor.memory", "1g")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import sqlContext._

    def parseRow(str: String): Option[AccelerometerData] = {
      val line = str.split(",")
      try {
        Some(AccelerometerData(line(0).toInt, line(1), line(2).toLong, line(3).toDouble, line(4).toDouble, line(5).toDouble))
      } catch {
        case e: Exception =>
          line foreach println
          None
      }
    }

    val textRDD = sc.textFile("/Users/debojit/code_bases/spark-mlib-project/data/data.csv")
    val accelerometerDataRDD = textRDD.flatMap(txt => parseRow(txt)).cache()
    val labeledPoints: List[LabeledPoint] = List.empty

    for (userId <- 1 to 2) {
      for (activity <- ACTIVITIES) {
        val times = accelerometerDataRDD
          .filter(_.userId == userId)
          .filter(_.activity == activity)
          .map(_.timestamp)
          .sortBy(x => x, true).cache()

        if (100 < times.count()) {
          val intervals = defineWindows(times)

          for (interval <- intervals) {
            var j = 0
            while (j < interval(0)) {
              val data: RDD[(Long, Double, Double, Double)] = accelerometerDataRDD
                .filter(_.userId == userId)
                .filter(_.activity == activity)
                .filter(acc =>
                  acc.timestamp < interval(1) + j * 5000000000L &&
                    acc.timestamp < interval(1) + (j - 1) * 5000000000L)
                .map(acc => (acc.timestamp, acc.acc_x, acc.acc_y, acc.acc_z))
                .sortBy(x => x, true).cache()

              if (data.count() > 0) {
                // transform into double array
                val doubles: RDD[Array[Double]] = data.map(acc => Array(acc._2, acc._3, acc._4))
                // transform into vector without timestamp
                val vectors: RDD[Vector] = doubles.map(Vectors.dense);
                // data with only timestamp and acc
                val timestamp: RDD[Array[Long]] = data.map(acc => Array(acc._1, acc._3.toLong))

                ////////////////////////////////////////
                // extract features from this windows //
                ////////////////////////////////////////

                // the average acceleration
                val mean = ExtractFeature.computeAvgAcc(vectors)
                // the variance
                val variance = ExtractFeature.computeVariance(vectors)
                // the average absolute difference
                val avgAbsDiff = ExtractFeature.computeAvgAbsDifference(doubles, mean)

                // the average resultant acceleration
                val resultant = ExtractFeature.computeResultantAcc(doubles)

                // the average time between peaks
                val avgTimePeak = ExtractFeature.computeAvgTimeBetweenPeak(timestamp, vectors)

                // Let's build LabeledPoint, the structure used in MLlib to create and a predictive model
                val labeledPoint: LabeledPoint = getLabeledPoint(activity, mean, variance, avgAbsDiff, resultant, avgTimePeak);

                labeledPoints :: List(labeledPoint)

              }
              j += 1
            }
          }
        }
      }
      if (labeledPoints.size > 0) {
        // data ready to be used to build the model
        val data: RDD[LabeledPoint] = sc.parallelize(labeledPoints)
        // Split data into 2 sets : training (60%) and test (40%).
        val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.6, 0.4))
        val trainingData: RDD[LabeledPoint] = splits(0).cache()
        val testData: RDD[LabeledPoint] = splits(1)

        // With DecisionTree
        val errDT = DecisionTrees.createModel(sc, trainingData, testData)
        println("sample size " + data.count())
        println("Test Error Decision Tree: " + errDT);
      }
    }

    def defineWindows(times: RDD[Long]): List[Array[Long]] = {
      val firstElement = times.first()
      val lastElement = times.sortBy(time => time, false, 1).first()
      // compute the difference between each timestamp
      val tsBoundariesDiff = PrepareData.boudariesDiff(times, firstElement, lastElement)

      // define periods of recording
      // if the difference is greater than 100 000 000, it must be different periods of recording
      // ({min_boundary, max_boundary}, max_boundary - min_boundary > 100 000 000)
      val jumps = PrepareData.defineJump(tsBoundariesDiff)

      // Now define the intervals
      PrepareData.defineInterval(jumps, firstElement, lastElement, 5000000000L)
    }

    /**
     * build the data set with label & features (11)
     * activity, mean_x, mean_y, mean_z, var_x, var_y, var_z, avg_abs_diff_x, avg_abs_diff_y, avg_abs_diff_z, res, peak_y
     */

    def getLabeledPoint(activity: String, mean: Array[Double], variance: Array[Double], avgAbsDiff: Array[Double], resultant: Double, avgTimePeak: Double): LabeledPoint = {
      val features: Array[Double] = Array(
        mean(0),
        mean(1),
        mean(2),
        variance(0),
        variance(1),
        variance(2),
        avgAbsDiff(0),
        avgAbsDiff(1),
        avgAbsDiff(2),
        resultant,
        avgTimePeak)

      val label = activity match {
        case "Jogging"    => 1
        case "Standing"   => 2
        case "Sitting"    => 3
        case "Upstairs"   => 4
        case "Downstairs" => 5
        case _            => 0
      }

      new LabeledPoint(label, Vectors.dense(features))
    }
    sc.stop
  }
}
