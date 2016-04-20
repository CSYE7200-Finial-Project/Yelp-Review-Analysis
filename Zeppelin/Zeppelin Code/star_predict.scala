// these codes need to run in zeppelin
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg._

class reviewPredict{ 
  val model = DecisionTreeModel.load(sc,"/Users/wanlima/Documents/apache/myDecisionTreeClassificationModel_2")
//   val review = "1:0.244 2:0.61 3:0.561 4:0.878 5:0.341"
  def vetorParser(line:String):Vector = { 
    val items = line.split(' ')
    val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt -1  // Convert 1-based indices to 0-based.
    val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip
    Vectors.sparse(indices.length+1, indices.toArray, values.toArray)
  }  
  def getPrediction(reviewPerc:String):Double = model.predict(vetorParser(reviewPerc))
}

import scala.collection.mutable
import java.io._
import org.apache.spark.rdd._
import sqlContext.implicits._
import org.apache.spark.SparkContext

class PreDataML {
  val reg = """^\((\w+)-?&?'?\w*?,(\d+)\)$""".r
  val reg1 = """^\(( ),(\d+)\)$""".r //( ,10)
  val reg2 = """^\((\$+?\d+?%?\w*?),(\d+)\)$""".r // ($10,8)
  val reg3 = """^\('\w*,(\d+)\)$""".r // ('n, 10)
  val reg4 = """^\((\d+%),(\d+)\)$""".r
  val reg5 = """^\((\$+?(\s)?),(\d+)\)$""".r //($$,55)
  val reg6 = """^\((\$+?\w+?),(\d+)\)$""".r //($in,8)
  val reg7 = """^\((\d+?\$+?),(\d+)\)$""".r
  val reg8 = """^\((\w+?\$+?),(\d+)\)$""".r //(a$$,12)
  def rdd2Tuple(rdd:org.apache.spark.rdd.RDD[String]): List[(String, Int)] = {
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
        case reg7(word, num) => (word, num.toInt)
        case reg8(word, num) => (word, num.toInt)
        case _ => ("",0)
      }
    } yield a
    b.toList
  }
  def triFracDouble(a: Int, b: Int): Double = (a.toDouble / b * 1000).round / 1000.toDouble
  def getPercent(seqSet: Seq[org.apache.spark.rdd.RDD[String]], reviewContext: String):List[Double] = {
    val set1 = rdd2Tuple(seqSet(0)).unzip._1
    val set2 = rdd2Tuple(seqSet(1)).unzip._1
    val set3 = rdd2Tuple(seqSet(2)).unzip._1
    val set4 = rdd2Tuple(seqSet(3)).unzip._1
    val set5 = rdd2Tuple(seqSet(4)).unzip._1
    val setTotal = (set1 ++ set2 ++ set3 ++ set4 ++ set5)
println(setTotal.size)
    // val res = sparkC.parallelize(reviewContext.split("[\\.,\\s!;?:\"]+")).collect()
    val res =reviewContext.split("[\\.,\\s!;?:\"]+")
    val words = res.intersect(setTotal)
    val totalSize = words.size
    println(totalSize)
    val prcnt1 = triFracDouble(words.intersect(set1).size, totalSize)
    val prcnt2 = triFracDouble(words.intersect(set2).size, totalSize)
    val prcnt3 = triFracDouble(words.intersect(set3).size, totalSize)
    val prcnt4 = triFracDouble(words.intersect(set4).size, totalSize)
    val prcnt5 = triFracDouble(words.intersect(set5).size, totalSize)
    List(prcnt1,prcnt2,prcnt3,prcnt4,prcnt5)
  } 
}

val seqRdd2 = for {
  i <- 1 to 5
  file = sc.textFile("/Users/wanlima/Dropbox/Scala/project/FinalProject_mac/src/main/resources/wordsDelCom/time5/word" + i + ".txt")
} yield file

val pdML2 = new PreDataML
//val reviewContext = z.input("review"," ").toString
println(reviewContext)
val perc = pdML2.getPercent(seqRdd2, reviewContext)
val reviewPerc = "1:" + perc(0) + " 2:" + perc(1) + " 3:" + perc(2) + " 4:" + perc(3) + " 5:" + perc(4)
val rp = new reviewPredict
val predictStar = rp.getPrediction(reviewPerc)
println("predict star: "+ predictStar)
println("%table star1\tstar2\tstar3\tstar4\tstar5\n"+perc(0)+"\t"+perc(1)+"\t"+perc(2)+"\t"+perc(3)+"\t"+perc(4)+"\n")

val reviewContext = z.input("review","").toString

