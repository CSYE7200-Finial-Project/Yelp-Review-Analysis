package neu.edu.scala.finalyelp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable
import java.io._
import org.apache.spark.rdd._
import com.norbitltd.spoiwo.model._
import org.joda.time.LocalDate
import com.norbitltd.spoiwo.natures.xlsx.Model2XlsxConversions._
import com.norbitltd.spoiwo.model.enums.CellFill

class PreDataML4 {
  def triFracDouble(a: Int, b: Int): Double = (a.toDouble / b * 1000).round / 1000.toDouble
  def getPercent(seqSet: Seq[RDD[String]], reviewContext: String, sparkC: SparkContext):List[Double] = {
    val ci = new CleanIdentical
    val set1 = ci.rdd2Tuple(seqSet(0)).unzip._1
    val set2 = ci.rdd2Tuple(seqSet(1)).unzip._1
    val set3 = ci.rdd2Tuple(seqSet(2)).unzip._1
    val set4 = ci.rdd2Tuple(seqSet(3)).unzip._1
    val set5 = ci.rdd2Tuple(seqSet(4)).unzip._1
    val setTotal = (set1 ++ set2 ++ set3 ++ set4 ++ set5)

    val res = sparkC.parallelize(reviewContext.split("[\\.,\\s!;?:\"]+")).collect()
    val words = res.intersect(setTotal)
    val totalSize = words.size
    val prcnt1 = triFracDouble(words.intersect(set1).size, totalSize)
    val prcnt2 = triFracDouble(words.intersect(set2).size, totalSize)
    val prcnt3 = triFracDouble(words.intersect(set3).size, totalSize)
    val prcnt4 = triFracDouble(words.intersect(set4).size, totalSize)
    val prcnt5 = triFracDouble(words.intersect(set5).size, totalSize)
    List(prcnt1,prcnt2,prcnt3,prcnt4,prcnt5)
  }
  def getRows(seqSet: Seq[RDD[String]], sourceFile: Seq[RDD[String]], sparkC: SparkContext) = {
    val writer = new PrintWriter(new File("src/main/resources/wordFreLine/wordFreLine2.txt"))
    val rr = for { i <- 0 to 4 } yield {
      val file0No = sourceFile(0).flatMap(lines => lines.split("/n")).collect().size
      val file1No = sourceFile(1).flatMap(lines => lines.split("/n")).collect().size
      val file2No = sourceFile(2).flatMap(lines => lines.split("/n")).collect().size
      val file3No = sourceFile(3).flatMap(lines => lines.split("/n")).collect().size
      val file4No = sourceFile(4).flatMap(lines => lines.split("/n")).collect().size

      val lines = sourceFile(i).flatMap(lines => lines.split("/n")).collect()
//      val lineNo = i match {
//        case 0 => 0
//        case 1 => file0No + 1
//        case 2 => file0No + file1No + 1
//        case 3 => file0No + file1No + file2No + 1
//        case 4 => file0No + file1No + file2No + file3No + 1
//      }      
      val rows = for {
        (line, lineNum) <- lines.zipWithIndex
        val percents = getPercent(seqSet, line, sparkC)

      } yield writer.write((i+1) + " 1:" + percents(0) + " 2:" + percents(1) + " 3:" + percents(2) + " 4:" + percents(3) + " 5:" + percents(4)  + "\n")
      rows
    }
    writer.close()
  }
}