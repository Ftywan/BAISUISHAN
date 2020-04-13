package task1

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount {
  val input1 = "src/main/scala/Task1/data/task1-input1.txt"
  val input2 = "src/main/scala/Task1/data/task1-input2.txt"
  val input3 = "src/main/scala/Task1/data/stopwords.txt"
  val output = "src/main/scala/Task1/output"

  def main (args: Array[String]): Unit = {
    // initialization of the spark program
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("Word Count")
    val sc = new SparkContext(conf)

    // I/O
    val list1 = sc.textFile(input1)
    val list2 = sc.textFile(input2)
    val stopWords = sc.textFile(input3).map(x => x.trim).collect().toSet

    val count1 = list1
      .flatMap(line => line
        .replaceAll("[^a-zA-Z'-]", " ")
        .split("\\s+"))
      .filter(word => !(word.equals("")))
      .filter(word => !(stopWords contains word.toLowerCase))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_+_).cache()


    val count2 = list2
      .flatMap(line => line
                .replaceAll("[^a-zA-Z'-]", " ")
                .split("\\s+"))
      .filter(word => !(word.equals("")))
      .filter(word => !(stopWords contains word.toLowerCase))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_+_).cache()

    val commonCount = count1.join(count2).map{case(word, (one, two)) => (word, Math.min(one,two))}.map(_.swap).sortByKey(false)

    FileUtils.deleteDirectory(new File(output))
    sc.parallelize(commonCount.take(15)).saveAsTextFile(output)
  }
}
