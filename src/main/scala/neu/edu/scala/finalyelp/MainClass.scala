package neu.edu.scala.finalyelp
import neu.edu.scala.finalyelp._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable
import scala.io._
/*
 * main class
 * @author:wanlima
 */
object MainClass {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("word count").setMaster("local")
    val sc = new SparkContext(conf)

    // 1. extract the review context from the reviews files,and divided into five files according to the star level
    val jpf = new JsonPaserFile
    val businessSourcePath = "/Users/wanlima/Documents/Scala/yelp_dataset_challenge_academic_dataset/business_test.json"
    val reviewSourcePath = "/Users/wanlima/Documents/Scala/yelp_dataset_challenge_academic_dataset/review_test.json"
    jpf.saveReviews(businessSourcePath, reviewSourcePath)

    // 2. word count for each star level (5 output)
    val swc = SimpleWordCount
    for (i <- 1 to 5) yield {
      val file = sc.textFile("src/main/resources/test" + i + ".csv")
      swc.saveWordCount(file, "wordcount" + i)
    }

    // 3. refine the word count result, clean the identical words which are in top position
    val ci = new CleanIdentical()
    val seqRdd = for {
      i <- 1 to 5
      file = sc.textFile("src/main/resources/wordcount" + i + "/part-00000")
    } yield file // get the sequence of the five original (word, count) RDD
    val identicalSet = ci.getIdenticalSet(seqRdd, 300)
    println(identicalSet)
    ci.writeFile(seqRdd(0), "/time0/word1", identicalSet)
    ci.writeFile(seqRdd(1), "/time0/word2", identicalSet)
    ci.writeFile(seqRdd(2), "/time0/word3", identicalSet)
    ci.writeFile(seqRdd(3), "/time0/word4", identicalSet)
    ci.writeFile(seqRdd(4), "/time0/word5", identicalSet)

    // clean three times more
    for (j <- 1 to 3) {
      val seqRdd = for {
        i <- 1 to 5
        file = sc.textFile("src/main/resources/wordsDelCom" + "/time" + (j - 1) + "/word" + i + ".txt")
      } yield file
      val identicalSet = ci.getIdenticalSet(seqRdd, 300)
      println(identicalSet)
      seqRdd(0).collect.foreach(print)
      ci.writeFile(seqRdd(0), "/time" + j + "/word1", identicalSet)
      ci.writeFile(seqRdd(1), "/time" + j + "/word2", identicalSet)
      ci.writeFile(seqRdd(2), "/time" + j + "/word3", identicalSet)
      ci.writeFile(seqRdd(3), "/time" + j + "/word4", identicalSet)
      ci.writeFile(seqRdd(4), "/time" + j + "/word5", identicalSet)
    }

    
    /* preDataML 
    for (i <- 1 to 5) {
      val pdML = new PreDataML()
      val file = sc.textFile("src/main/resources/wordsDelCom/time3/word" + i + ".txt")
      val set = ci.rdd2Tuple(file).unzip._1.toSet
      val review = sc.textFile("src/main/resources/test" + i + ".csv")
      pdML.wordsToRow0(set)
      pdML.saveToxlsx(review, set, sc, i)
    }
    * 
    */
    // 4. predata for machine learning, get the required input formated data for training data
    val review = for {
      i <- 1 to 5
      file =sc.textFile("src/main/resources/test" + i + ".csv")
    } yield file
    
    val seqRdd2 = for {
      i <- 1 to 5
      file = sc.textFile("src/main/resources/wordsDelCom/time3/word" + i + ".txt")
    } yield file

    val pdML2 = new PreDataML4
    val rows = pdML2.getRows(seqRdd2, review, sc)
//    pdML2.saveToXlsx(rows)
  }
}