package neu.edu.scala.predict

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg._
object reviewPredict extends App{
  
  val conf = new SparkConf().setAppName("word count").setMaster("local[4]")
    val sc = new SparkContext(conf)
  val model = DecisionTreeModel.load(sc, "target/temp/myDecisionTreeClassificationModel")
  
  val review = "1:0.244 2:0.61 3:0.561 4:0.878 5:0.341"
  def vetorParser(line:String):Vector = { 
    val items = line.split(' ')
    
    val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt -1  // Convert 1-based indices to 0-based.
    val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip
    
    Vectors.sparse(indices.length+1, indices, values)
  }
  val veviewVector = vetorParser(review)
  
  val prediction = model.predict(veviewVector)
  print(prediction)
}