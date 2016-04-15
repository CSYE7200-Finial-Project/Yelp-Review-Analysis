package neu.edu.scala.finalyelp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.collection.mutable
import scala.io._

/*
 * a simple test file which can do a simple word count in sparl
 * @author:wanlima
 */

object SimpleWordCount {

  val ignoreSet = mutable.Set.empty[String]
  val sign = Set("-", ",", "&", "\"", ".", "", "--", ":", "*", ";-", "$", "#", "n","+")
  ignoreSet ++= sign
  // add the stop word into the ignoreSet
  val stopWordFile = Source.fromFile("src/main/resources/stop-word-list.csv")
  for (line <- stopWordFile.getLines) {
    val words = line.split(",").map(_.trim)
    ignoreSet ++= words
  }
  def getWordCount(input: RDD[String]): RDD[(String, Int)] =
    input.flatMap(line => line.split("[\\.,\\s!;?:\"]+"))
      .filter(word => (!ignoreSet.exists(x => x.equalsIgnoreCase(word))))
      .map(word => (word, 1))
      .reduceByKey(_ + _, 1)
      .map(item => item.swap)
      .sortByKey(false, 1)
      .map(item => item.swap)
  def saveWordCount(input: RDD[String], ouput: String): Unit = {
    val wordCounts = getWordCount(input)
    wordCounts.saveAsTextFile("src/main/resources/" + ouput)
  }
}