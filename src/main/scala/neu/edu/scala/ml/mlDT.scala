package neu.edu.scala.ml

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import fileParser._
 
     
   
object mlDT extends App{
  
   val conf = new SparkConf().setAppName("mlDT")
                             .setMaster("local[4]")
                             
    val sc = new SparkContext(conf)
   
    // Load and parse the data file.
val data = MLUtils.loadLibSVMFile(sc, "testData/wordFreLine2.txt")
// Split the data into training and test sets (30% held out for testing)


val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

print("labledpoint: " + data.toString)

// Train a DecisionTree model.
//  Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 6
val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "gini"
val maxDepth = 5
val maxBins = 30

print("start training model")
   
val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
  impurity, maxDepth, maxBins)

  
print("training complete")
// Evaluate model on test instances and compute test error
val labelAndPreds = testData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
   
   print("evaluate")
val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
println("Test Error = " + testErr)
println("Learned classification tree model:\n" + model.toDebugString)

// Save and load model
model.save(sc, "target/temp/myDecisionTreeClassificationModel")
val sameModel = DecisionTreeModel.load(sc, "target/temp/myDecisionTreeClassificationModel")
}
