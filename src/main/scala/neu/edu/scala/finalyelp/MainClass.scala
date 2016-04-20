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

    /*
     * 1. extract the review context from the reviews files,and divided into five files according to the star level
     * because the data size is large, here is just run the test data
     */  
    val jpf = new JsonPaserFile
    val businessSourcePath = "/Users/wanlima/Documents/Scala/workspace/Yelp-Review-Analysis/src/main/resources/business_test.json"
    val reviewSourcePath = "/Users/wanlima/Documents/Scala/workspace/Yelp-Review-Analysis/src/main/resources/review_test.json"
    jpf.saveReviews(businessSourcePath, reviewSourcePath)

    //2. word count for each star level (5 output)
    val swc = SimpleWordCount
    for (i <- 1 to 5) yield {
      val file = sc.textFile("src/main/resources/reviewContext/star" + i + ".csv")
      swc.saveWordCount(file, "wordcount" + i)
    }

    /* 3. refine the word count result, clean the identical words which are in top position
     * algorithm: 1. extract the identical words in the top 300  of the five wordcount files.
     *            2. remove the identical words from each wordcount files. 
     *            3. do step 1 and 2 for several time.
     */
    val ci = new CleanIdentical()
    val seqRdd = for {
      i <- 1 to 5
      file = sc.textFile("src/main/resources/wordcount" + i + "/part-00000")
    } yield file // get the sequence of the five original (word, count) RDD
    val identicalSet = ci.getIdenticalSet(seqRdd, 500)
    val identical04 = ci.getIdenticalSetTwo(seqRdd(0), seqRdd(4), 500)
    val identical13 = ci.getIdenticalSetTwo(seqRdd(1), seqRdd(3), 500)
    ci.writeFile(seqRdd(0), "/time0/word1", identicalSet ++ identical04)
    ci.writeFile(seqRdd(1), "/time0/word2", identicalSet ++ identical13)
    ci.writeFile(seqRdd(2), "/time0/word3", identicalSet)
    ci.writeFile(seqRdd(3), "/time0/word4", identicalSet ++ identical13)
    ci.writeFile(seqRdd(4), "/time0/word5", identicalSet ++ identical04)

    // clean three times more
    for (j <- 1 to 4) {
      val seqRdd = for {
        i <- 1 to 5
        file = sc.textFile("src/main/resources/wordsDelCom" + "/time" + (j - 1) + "/word" + i + ".txt")
      } yield file
      val identicalSet = ci.getIdenticalSet(seqRdd, 500)
      val identical04 = ci.getIdenticalSetTwo(seqRdd(0), seqRdd(4), 500)
      val identical13 = ci.getIdenticalSetTwo(seqRdd(1), seqRdd(3), 500)
      ci.writeFile(seqRdd(0), "/time" + j + "/word1", identicalSet ++ identical04)
      ci.writeFile(seqRdd(1), "/time" + j + "/word2", identicalSet ++ identical13)
      ci.writeFile(seqRdd(2), "/time" + j + "/word3", identicalSet)
      ci.writeFile(seqRdd(3), "/time" + j + "/word4", identicalSet ++ identical13)
      ci.writeFile(seqRdd(4), "/time" + j + "/word5", identicalSet ++ identical04)
    }

    // clean last time
    val seqRdd3 = for {
      i <- 1 to 5
      file = sc.textFile("src/main/resources/wordsDelCom/time4/word" + i + ".txt")
    } yield file
    val identicalSet_3 = ci.getIdenticalSet(seqRdd3, 500)
    val identical04_3 = ci.getIdenticalSetTwo(seqRdd3(0), seqRdd3(4), 500)
    val identical13_3 = ci.getIdenticalSetTwo(seqRdd3(1), seqRdd3(3), 500)
    ci.writeFile_Last(seqRdd3(0), "/time5/word1", identicalSet_3 ++ identical04_3)
    ci.writeFile_Last(seqRdd3(1), "/time5/word2", identicalSet_3 ++ identical13_3)
    ci.writeFile_Last(seqRdd3(2), "/time5/word3", identicalSet_3)
    ci.writeFile_Last(seqRdd3(3), "/time5/word4", identicalSet_3 ++ identical13_3)
    ci.writeFile_Last(seqRdd3(4), "/time5/word5", identicalSet_3 ++ identical04_3)

    //4. predata for machine learning, get the required input formated data for training data
    val ci1 = new CleanIdentical
    val review = for {
      i <- 1 to 5
      file = sc.textFile("src/main/resources/reviewContext/star" + i + ".csv")
    } yield file

    val seqRdd2 = for {
      i <- 1 to 5
      file = sc.textFile("src/main/resources/wordsDelCom/time5/word" + i + ".txt")
    } yield file
    val set1 = ci1.rdd2Tuple(seqRdd2(0)).unzip._1
    val set2 = ci1.rdd2Tuple(seqRdd2(1)).unzip._1
    val set3 = ci1.rdd2Tuple(seqRdd2(2)).unzip._1
    val set4 = ci1.rdd2Tuple(seqRdd2(3)).unzip._1
    val set5 = ci1.rdd2Tuple(seqRdd2(4)).unzip._1
    val seqList = List(set1, set2, set3, set4, set5)
    val pdML2 = new PreDataML4
    val rows = pdML2.getRows(seqList, review, sc)

  }
}