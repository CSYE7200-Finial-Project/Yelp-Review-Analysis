package neu.edu.scala.finalyelp

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable

/*
 * a simple test file which can do a simple word count in sparl
 * @author:wanlima
 */

object SimpleWordCount extends App {
  val conf = new SparkConf().setAppName("word count").setMaster("local")
  val sc = new SparkContext(conf)
/*
      val pronoun = Set("I", "you", "she", "he", "it", "we", "they", "me")
      val article = Set("a", "an", "the")
      val adverb = Set("when", "where", "why", "what", "how")
      val preposition = Set("from", "to", "until", "over", "with", "after")
      val conjunction = Set("and", "but", "or", "nor", "too","")
      val ignoreSet = mutable.Set.empty[String]
      ignoreSet ++= pronoun ++= article ++= adverb ++= preposition ++= conjunction
*/
  val file = sc.textFile("src/main/resources/test5.csv")
  val wordCounts = file.flatMap(line => line.split(" "))
    //      .map(
    //        word => if (!ignoreSet.exists(x => x.equalsIgnoreCase(word))) {
    //          (word, 1)
    //        } else {
    //          (word, 0)
    //        })
    .map(word => (word, 1))
    .reduceByKey(_ + _, 1) 
    .map(item => item.swap) 
    .sortByKey(false, 1) 
    .map(item => item.swap)
  wordCounts.saveAsTextFile("src/main/resources/words5")

}