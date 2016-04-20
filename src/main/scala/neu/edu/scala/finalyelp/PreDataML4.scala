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
  def getPercent(seqSet: List[List[String]], reviewContext: String, sparkC: SparkContext):List[Double] = {
    val setTotal = seqSet.flatten
    val res = sparkC.parallelize(reviewContext.split("[\\.,\\s!;?:\"]+")).collect()
    val words = res.intersect(setTotal)
    val totalSize = words.size
    val prcnt1 = triFracDouble(words.intersect(seqSet(0)).size, totalSize)
    val prcnt2 = triFracDouble(words.intersect(seqSet(1)).size, totalSize)
    val prcnt3 = triFracDouble(words.intersect(seqSet(2)).size, totalSize)
    val prcnt4 = triFracDouble(words.intersect(seqSet(3)).size, totalSize)
    val prcnt5 = triFracDouble(words.intersect(seqSet(4)).size, totalSize)
    List(prcnt1,prcnt2,prcnt3,prcnt4,prcnt5)
  }
  def getRows(seqSet: List[List[String]], sourceFile: Seq[RDD[String]], sparkC: SparkContext) = {
    val writer = new PrintWriter(new File("src/main/resources/wordFreLine/wordFreLine_test1.txt"))
    val rr = for { i <- 0 to 4 } yield {
      val lines = sourceFile(i).flatMap(lines => lines.split("/n")).collect()
      val rows = for {
        (line, lineNum) <- lines.zipWithIndex
        val percents = getPercent(seqSet, line, sparkC)
      } yield writer.write((i+1) + " 1:" + percents(0) + " 2:" + percents(1) + " 3:" + percents(2) + " 4:" + percents(3) + " 5:" + percents(4)  + "\n")
      rows
    }
    writer.close()
  }
}