package neu.edu.scala.finalyelp
import neu.edu.scala.finalyelp._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable
import scala.io._
/*
 * this file can clean the reviews of Germany City Karlsruhe's business 
 * and print out 5 files with review context based on the different stars
 * @author:wanlima
 */
object MainClass {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("word count").setMaster("local[4]")
    val sc = new SparkContext(conf)
    
    // extract the review context from the reviews files,and divided into five files according to the star level
    val jpf = new JsonPaserFile
    val businessSourcePath = "/Users/wanlima/Documents/Scala/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json"
    val reviewSourcePath = "/Users/wanlima/Documents/Scala/yelp_dataset_challenge_academic_dataset/reviews/xae.json"
    jpf.saveReviews(businessSourcePath, reviewSourcePath)

    // word count for each star level (5 output)
    val swc = SimpleWordCount
    for (i <- 1 to 5) yield {
      val file = sc.textFile("src/main/resources/test" + i + ".csv")
      swc.saveWordCount(file, "wordcount" + i)
    }

    /* refine the word count result, clean the identical words which are in top position
     * algorithm: 1. extract the identical words in the top 300  of the five wordcount files.
     *            2. remove the identical words from each wordcount files. 
     *            3. do step 1 and 2 for several time.
     */
    val ci = new CleanIdentical()
    val seqRdd = for {
      i <- 1 to 5
      file = sc.textFile("src/main/resources/wordcount" + i + "/part-00000")
    } yield file // get the sequence of the five original (word, count) RDD
    val identicalSet = ci.getIdenticalSet(seqRdd, 7500)
    val identical04 = ci.getIdenticalSetTwo(seqRdd(0), seqRdd(4), 7500)
    val identical13 = ci.getIdenticalSetTwo(seqRdd(1), seqRdd(3), 7500)
    ci.writeFile(seqRdd(0), "/time0/word1", identicalSet ++ identical04)
    ci.writeFile(seqRdd(1), "/time0/word2", identicalSet ++ identical13)
    ci.writeFile(seqRdd(2), "/time0/word3", identicalSet)
    ci.writeFile(seqRdd(3), "/time0/word4", identicalSet ++ identical13)
    ci.writeFile(seqRdd(4), "/time0/word5", identicalSet ++ identical04)

    // clean three times more
    for (j <- 1 to 5) {
      val seqRdd = for {
        i <- 1 to 5
        file = sc.textFile("src/main/resources/wordsDelCom" + "/time" + (j - 1) + "/word" + i + ".txt")
      } yield file
      val identicalSet = ci.getIdenticalSet(seqRdd, 7500)
      val identical04 = ci.getIdenticalSetTwo(seqRdd(0), seqRdd(4), 7500)
      val identical13 = ci.getIdenticalSetTwo(seqRdd(1), seqRdd(3), 7500)
      seqRdd(0).collect.foreach(print)
      ci.writeFile(seqRdd(0), "/time" + j + "/word1", identicalSet ++ identical04)
      ci.writeFile(seqRdd(1), "/time" + j + "/word2", identicalSet ++ identical13)
      ci.writeFile(seqRdd(2), "/time" + j + "/word3", identicalSet)
      ci.writeFile(seqRdd(3), "/time" + j + "/word4", identicalSet ++ identical13)
      ci.writeFile(seqRdd(4), "/time" + j + "/word5", identicalSet ++ identical04)
    }
/*
    //predata for machine learning, get the required input formated data for training data
    val ci1 = new CleanIdentical
    val review = for {
      i <- 1 to 5
      file = sc.textFile("src/main/resources/test" + i + ".csv")
    } yield file

    val seqRdd2 = for {
      i <- 1 to 5
      file = sc.textFile("src/main/resources/wordsDelCom/time3/word" + i + ".txt")
    } yield file
    val set1 = ci1.rdd2Tuple(seqRdd2(0)).unzip._1
    val set2 = ci1.rdd2Tuple(seqRdd2(1)).unzip._1
    val set3 = ci1.rdd2Tuple(seqRdd2(2)).unzip._1
    val set4 = ci1.rdd2Tuple(seqRdd2(3)).unzip._1
    val set5 = ci1.rdd2Tuple(seqRdd2(4)).unzip._1
    val seqList = Seq(set1,set2,set3,set4,set5)
    //println(set1.size)
    val pdML2 = new PreDataML4
    val rows = pdML2.getRows(seqList, review, sc)
    //pdML2.saveToXlsx(rows)
*/

    //    val reviewContext = ""
    //    val percents = pdML2.getPercent(seqRdd2, reviewContext, sc)
    //    percents.foreach(println)

    /*
    for (i <- 1 to 5) {
      val pdML = new PreDataML()
      val file = sc.textFile("src/main/resources/wordsDelCom/time3/word" + i + ".txt")
      val set = ci.rdd2Tuple(file).unzip._1.toSet
      val review = sc.textFile("src/main/resources/test" + i + ".csv")
      pdML.wordsToRow0(set)
      pdML.saveToxlsx(review, set, sc, i)
    }
*/
  }
}