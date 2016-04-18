package neu.edu.scala.ml

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}

 object fileParser {
  
    
   def parseRecord(line:String):(Int, Array[Int], Array[Double])= {
    val items = line.split("\\s")
    val label = items(items.length-1).toInt
    val columns = items.dropRight(1);
    var i = 0
    
   
    var indices = Array[Int]()
    var valuesTemp = Array[Any]()
    
    while (i<columns.length){
      indices = indices:+ i
      valuesTemp = valuesTemp:+ columns(i)
      i= i+1
    } 
    val values = valuesTemp.map { case d:String => d.toDouble}
    (label, indices, values)
  }
   
   def parseFile(
      sc: SparkContext,
      path: String,
      minPartitions: Int): RDD[(Int, Array[Int], Array[Double])] = {
     sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map(parseRecord)
   }
   
   def loadFile(sc: SparkContext, path: String) : RDD[LabeledPoint] = {
      val parsed = parseFile(sc, path, 1)
      
      parsed.map { case (label, indices, values) =>
      LabeledPoint(label, Vectors.sparse(indices.length, indices, values))
    }
   }
}