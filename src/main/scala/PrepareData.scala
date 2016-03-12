
import org.apache.spark.rdd.RDD
object PrepareData {

  def boudariesDiff(timestamps: RDD[Long], firstElement: Long, lastElement: Long): RDD[(Array[Long], Long)] = {
    val firstRDD = timestamps.filter(record => record > firstElement)
    val secondRDD = timestamps.filter(record => record < lastElement)
    // define periods of recording
    firstRDD.zip(secondRDD)
      .map { case (first, second) => (Array(first, second), first - second) }
  }

  def defineJump(tsBoundaries: RDD[(Array[Long], Long)]): RDD[(Long, Long)] = {
    tsBoundaries.filter(pair => pair._2 > 100000000)
      .map { pair => (pair._1(1), pair._1(0)) }
  }

  // (min, max)
  def defineInterval(tsJump: RDD[(Long, Long)], firstElement: Long, lastElement: Long, windows: Long): List[Array[Long]] = {

    val flatten: List[Long] = tsJump.flatMap(pair => Array(pair._1, pair._2))
      .sortBy(t => t, true, 1)
      .collect().toList

    val size = flatten.size
    val results = new scala.collection.mutable.ArrayBuffer[Array[Long]]()
    results += Array(firstElement, flatten(0), Math.round((flatten(0) - firstElement) / windows))

    for (i <- 1 until size - 1 by 2) {
      results += Array(flatten(i), flatten(i + 1), Math.round((flatten(i + 1) - flatten(i)) / windows))
    }
    results += Array(flatten(size - 1), lastElement, Math.round((lastElement - flatten(size - 1)) / windows))
    results.toList
  }
}