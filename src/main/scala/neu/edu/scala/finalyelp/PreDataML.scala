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
/*
 * predata for machine learning, get the required input formated data for training data
 * @author: wanlima
 */

class PreDataML  {
  var workbook = Workbook()
  var allCells = Iterator[Cell]()
  var row1 = Iterator[Row]()

  var allWords = Iterator[Cell]()
  var allRows = Iterator[Row]()
  
  def addToIterator(cells: Iterator[Cell], cell: Iterator[Cell]): Iterator[Cell] = {
    cells ++ cell
  }
  def addToIterator2(rows: Iterator[Row], row: Iterator[Row]): Iterator[Row] = {
    rows ++ row
  }

  def wordsToRow0(set5: Set[String]) = {

    for ((word, i) <- set5.toList.zipWithIndex) {
      val cell = Cell(word, index = i + 1)
      allCells = addToIterator(allCells, Iterator(cell))
      if (i == set5.size - 1) {
        val row = Row(index = 0).addCells(allCells.toIterable)
        val sheet = Sheet(name = "sheet2").withRows(row)
        workbook = workbook.addSheet(sheet)
        row1 = addToIterator2(row1, Iterator(row))
      }
    }
  }
 
  def saveToxlsx(file5:RDD[String],set:Set[String],sparkC:SparkContext,i:Int) = {
    for ((line, lineNum) <- file5.flatMap(lines => lines.split("/n")).collect().zipWithIndex) yield {
      val res = sparkC.parallelize(line.split("[\\.,\\s!;?:\"]+"))
        .filter(word => set.exists(x => x.equalsIgnoreCase(word)))
        .map(word => (word, 1))
        .reduceByKey(_ + _, 1)
        .map(item => item.swap)
        .sortByKey(false, 1)
        .map(item => item.swap).collect()
        
      allWords = Iterator[Cell]()
      for (((word, num), i) <- res.zipWithIndex) yield {
        val position = workbook.sheets.head.rows.head.cells.toList.filter(cell => cell.value.toString.equalsIgnoreCase(word)) match {
          case List(cell) => cell.index.get
        }
        val cell = Cell(num, index = position)
        allWords = addToIterator(allWords, Iterator(cell))

        if (i == res.size - 1) {
          val row = Row(index = lineNum + 1).addCells(allWords.toIterable)
          allRows = addToIterator2(allRows, Iterator(row))
        }
      }//small for
      
    }//big for
    val sheet = Sheet("sheet1").addRows(allRows.toIterable).addRows(row1.toIterable)
    Workbook().addSheet(sheet).saveAsXlsx("src/main/resources/preDataML/predata" + i + ".xlsx")
  }//end def saveAsxlsx

}

