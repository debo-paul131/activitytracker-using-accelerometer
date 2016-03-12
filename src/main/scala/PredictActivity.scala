
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.model.DecisionTreeModel

object PredictActivity {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Users Activity Tracker")
      .set("spark.executor.memory", "1g")

    val sc = new SparkContext(conf)
    println(predict(sc))
  }

  def predict(sc: SparkContext): Double = {
    val model = DecisionTreeModel.load(sc, "actitracker")
    val feature = Array(
      3.3809183673469394, -6.880102040816324, 0.8790816326530612,
      50.08965378708187, 84.13105050494424, 20.304453787081833,
      5.930491461890875, 7.544194085797583, 3.519248229904206,
      12.968485972481643, 7.50031E8)
    val sample = Vectors.dense(feature)
    model.predict(sample)
  }
}