import java.io.File

import com.twitter.chill.Output
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions

import annotation.tailrec
import scala.reflect.ClassTag

/** A raw posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object Assignment2 extends Assignment2 {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Assignment2")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  val output = "src/main/scala/Task2/output"
  val input = "src/main/scala/Task2/data/QA_data.csv"

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile(input)
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored).cache()

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)

    FileUtils.deleteDirectory(new File((output)))
    sc.parallelize(results).saveAsTextFile(output)
  }
}

/** The parsing and kmeans methods */
class Assignment2 extends Serializable {

  /** Languages */
  val Domains =
    List(
      "Machine-Learning", "Compute-Science", "Algorithm", "Big-Data", "Data-Analysis", "Security", "Silicon Valley", "Computer-Systems",
      "Deep-learning", "Internet-Service-Providers", "Programming-Language", "Cloud-services", "Software-Engineering", "Embedded-System", "Architecture")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def DomainSpread = 50000

  assert(DomainSpread > 0)

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria, if changes of all centriods < kmeansEta, stop */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120

  // Parsing methods:

  /**
   * Load postings from the given file
   * @param lines: (type, id, parent id, score, domain(tag) )
   * @return postings: Posting(raw), Posting Objects
   */
  def rawPostings(lines: RDD[String]): RDD[Posting] = {
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType = arr(0).toInt,
        id = arr(1).toInt,
        //              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId = if (arr(2) == "") None else Some(arr(2).toInt),
        score = arr(3).toInt,
        tags = if (arr.length >= 5) Some(arr(4).intern()) else None)
    })
  }

  /**
   * Group the postings based on question id
   * @param postings: Posting Objects that represents the original data
   * @return questionGroups: a record containing (id, larger score between the two joining records, tag index in the list)
   */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Int, Int)] = {
    // Filter the questions and answers separately
    // Prepare them for a join operation by extracting the QID value in the first element of a tuple.
    val questions = postings.filter(posting => posting.postingType == 1).map(posting => (posting.id, (posting.score, posting.tags)))
    val answers = postings.filter(posting => posting.postingType == 2).map(posting => (posting.parentId.get, posting.score))
    // map to (id, larger score, tag index)
    val questionGroups = questions.join(answers).map(question => (question._1, Math.max(question._2._1._1, question._2._2), Domains.indexOf(question._2._1._2.get)))
    return questionGroups

  }

  /**
   * Compute the maximum score for each posting
   * @param groupedPostings: (id, score, tag index)
   * @return result: list of scored postings: (tag index, score)
   */
  def scoredPostings(groupedPostings: RDD[(Int, Int, Int)]): RDD[(Int, Int)] = {
      // in: id, score, tag
      // out: tag index, score
      // we discard the id here as it will be no more used
      val result = groupedPostings.map(posting => ((posting._1, posting._3), posting._2)).groupByKey().map(posting => (posting._1._2, posting._2.max))
      return  result
    }

  /**
   * Compute the vectors for the kmeans
   * @param scoredPosting: (tag index, score)
   * @return vectors: (D * X, S)
   */
  def vectorPostings(scoredPosting: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    return scoredPosting.map(scorePair => (DomainSpread * scorePair._1, scorePair._2))
  }

  // Kmeans method:
  /**
   * randomizes some vectors to be the initial centroids
   * @param vectors: vectors of (DX, S)
   * @return an Array of vectors
   */
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] =
    vectors.takeSample(false, kmeansKernels)

  /** Main kmeans computation */
  final def kmeans(randomCentroids: Array[(Int, Int)], points: RDD[(Int, Int)], debug: Boolean): Array[(Int, Int)] =  {
    var centroids = randomCentroids
    var isConverged = false
    var iteration: Int = 0

    while((!isConverged) && iteration <= kmeansMaxIterations) {
      // get the nearest centroid and pair with the point(index of centroid, point)
      val cluster = points.map(point => (findClosest(point, centroids), point)).groupByKey().map(pair => (pair._1, averageVectors(pair._2))).collect()

      // update centroids and distance value
      val newCentroids = centroids.clone()
      for(pair <- cluster) newCentroids.update(pair._1, pair._2)
      val distance = euclideanDistance(centroids, newCentroids)
      isConverged = converged(distance)
      iteration += 1
      centroids = newCentroids
    }
    return centroids
  }

  /**
   * generates the organized results
   * @param centroids: calculated centroids
   * @param points: vectors in the space
   * @return organized (centroid, size, median, average) tuple
   */
  def clusterResults(centroids: Array[(Int, Int)], points: RDD[(Int, Int)]): Array[((Int, Int), Int, Double, Double)] ={
    val cluster = points.map(point => (findClosest(point, centroids), point)).groupByKey()
    val result = cluster.map { group =>
      val centroid = centroids(group._1)
      val points = group._2
      val size = points.size
      val scores = points.toArray.sortBy(point => point._2)
      val median: Double = if (size % 2 == 0) (scores(size / 2 - 1)._2 + scores(size / 2)._2) / 2 else scores(size / 2)._2
      val average = scores.map(point => point._2).toArray.sum.toDouble / size

      (centroid, size, median, average)
    }
    result.collect().toArray.sortBy(param => param._2).reverse
  }


  //
  //
  //  Kmeans utilities (Just some cases, you can implement your own utilities.)
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) = distance < kmeansEta

  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while (idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  def computeMedian(a: Iterable[(Int, Int)]) = {
    val s = a.map(x => x._2).toArray
    val length = s.length
    val (lower, upper) = s.sortWith(_ < _).splitAt(length / 2)
    if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  /**
   * print out the results
   * @param results: results to display
   */
  def printResults(results: Array[((Int, Int), Int, Double, Double)]): Array[((Int, Int), Int, Double, Double)] = {
    results.foreach{ result =>
      println("Domain: " + Domains(result._1._1 / DomainSpread) + ", centroid: " + result._1 + ", size: " + result._2 + ", score sum: " + result._3 + ", average: " + result._4)
    }
    results
  }
}