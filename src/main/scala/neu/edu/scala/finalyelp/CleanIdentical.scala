package neu.edu.scala.finalyelp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable
import scala.collection.mutable
import java.io._
import org.apache.spark.rdd._
    /* 
   	 * refine the word count result, clean the identical words which are in top position
     * algorithm: 1. extract the identical words in the top 300  of the five wordcount files.
     *            2. remove the identical words from each wordcount files. 
     *            3. do step 1 and 2 for several time.
     * @author:wanlima
     */
class CleanIdentical extends Serializable {

  val reg = """^\((\w+)-?&?'?\w*?,(\d+)\)$""".r
  val reg1 = """^\((:\)),(\d+)\)$""".r
  val reg2 = """^\((\$+?\d+?%?\w*?),(\d+)\)$""".r // ($10,8)
  val reg3 = """^\('\w*,(\d+)\)$""".r // ('n, 10)
  val reg4 = """^\((\d+%),(\d+)\)$""".r
  val reg5 = """^\((\$+?),(\d+)\)$""".r
  val reg6 = """^\((\$+?\w+?),(\d+)\)$""".r //($in,8)

  def getIdenticalSet(seqSet: Seq[RDD[String]], topNum: Int): Set[String] = {
    val set1 = rdd2Tuple(seqSet(0)).unzip._1
    val set2 = rdd2Tuple(seqSet(1)).unzip._1
    val set3 = rdd2Tuple(seqSet(2)).unzip._1
    val set4 = rdd2Tuple(seqSet(3)).unzip._1
    val set5 = rdd2Tuple(seqSet(4)).unzip._1

    val identicalSet = set1.take(topNum).intersect(set2.take(topNum))
      .intersect(set3.take(topNum))
      .intersect(set4.take(topNum))
      .intersect(set5.take(topNum))
    identicalSet.toSet
  }

  def rdd2Tuple(rdd: RDD[String]): List[(String, Int)] = {
    val b = for {
      line <- rdd.collect()
      val a = line match {
        case reg(word, num)  => (word, num.toInt)
        case reg1(word, num) => (word, num.toInt)
        case reg2(word, num) => (word, num.toInt)
        case reg3(word, num) => (word, num.toInt)
        case reg4(word, num) => (word, num.toInt)
        case reg5(word, num) => (word, num.toInt)
        case reg6(word, num) => (word, num.toInt)
      }
    } yield a
    b.toList
  }

  def writeFile(inputSet: RDD[String], outputFile: String, setIdentical: Set[String]) = {

    val writer = new PrintWriter(new File("src/main/resources/wordsDelCom/" + outputFile + ".txt"))
    val source = inputSet.collect
    source.foreach(line => line match {
      case reg(word, num)  => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
      case reg1(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
      case reg2(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
      case reg3(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
      case reg4(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
      case reg5(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
      case reg6(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
    })
    writer.close()
  }

}