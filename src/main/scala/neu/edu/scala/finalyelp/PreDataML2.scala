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

class PreDataML2 {

  def getPercent(a: Int, b: Int): Double = (a.toDouble / b * 1000).round / 1000.toDouble

  def getRows(seqSet: Seq[RDD[String]], sourceFile: Seq[RDD[String]], sparkC: SparkContext): Iterable[Row] = {
    val ci = new CleanIdentical
    val set1 = ci.rdd2Tuple(seqSet(0)).unzip._1
    val set2 = ci.rdd2Tuple(seqSet(1)).unzip._1
    val set3 = ci.rdd2Tuple(seqSet(2)).unzip._1
    val set4 = ci.rdd2Tuple(seqSet(3)).unzip._1
    val set5 = ci.rdd2Tuple(seqSet(4)).unzip._1
    val setTotal = (set1 ++ set2 ++ set3 ++ set4 ++ set5)
    val rr = for { i <- 0 to 4 } yield {
      val file0No = sourceFile(0).flatMap(lines => lines.split("/n")).collect().size
      val file1No = sourceFile(1).flatMap(lines => lines.split("/n")).collect().size
      val file2No = sourceFile(2).flatMap(lines => lines.split("/n")).collect().size
      val file3No = sourceFile(3).flatMap(lines => lines.split("/n")).collect().size
      val file4No = sourceFile(4).flatMap(lines => lines.split("/n")).collect().size
      
      val lines = sourceFile(i).flatMap(lines => lines.split("/n")).collect()
      val lineNo = i match {
        case 0 => 0
        case 1 =>  file0No + 1
        case 2 =>  file0No + file1No + 1
        case 3 =>  file0No + file1No + file2No + 1
        case 4 =>  file0No + file1No + file2No + file3No + 1
      }
      
      val rows = for {
        (line, lineNum) <- lines.zipWithIndex
        val res = sparkC.parallelize(line.split("[\\.,\\s!;?:\"]+")).collect()
        val words = res.intersect(setTotal)
        val totalSize = words.size
        val prcnt1 = getPercent(words.intersect(set1).size, totalSize)
        val prcnt2 = getPercent(words.intersect(set2).size, totalSize)
        val prcnt3 = getPercent(words.intersect(set3).size, totalSize)
        val prcnt4 = getPercent(words.intersect(set4).size, totalSize)
        val prcnt5 = getPercent(words.intersect(set5).size, totalSize)
        val cell0 = Cell(lineNum + lineNo + 1, index = 0)
        val cell1 = Cell(prcnt1, index = 1)
        val cell2 = Cell(prcnt2, index = 2)
        val cell3 = Cell(prcnt3, index = 3)
        val cell4 = Cell(prcnt4, index = 4)
        val cell5 = Cell(prcnt5, index = 5)
        val cell6 = Cell(i + 1, index = 6)
      } yield Row(index = lineNum + lineNo + 1).withCells(cell0, cell1, cell2, cell3, cell4, cell5, cell6)
      rows
    }
    val r = rr(0) ++ rr(1) ++ rr(2) ++ rr(3) ++ rr(4)
    
    r.toIterable
  }

  def saveToXlsx(rows: Iterable[Row]) = {
    val row0 = Row(index = 0).withCellValues("Review", 1, 2, 3, 4, 5, "Stars")
    val sheet = Sheet().addRow(row0).addRows(rows)
    val workbook = Workbook().addSheet(sheet)
    workbook.saveAsXlsx("src/main/resources/preDataML/predata.csv")
  }

}