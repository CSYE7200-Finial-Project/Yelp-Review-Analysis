package neu.edu.scala.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.{Row, SQLContext}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import fileParser._

object mlNerualNetwork extends App {
  
  val conf = new SparkConf().setAppName("word count").setMaster("local[4]")
    val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  
 val data = sqlContext.read.format("libsvm")
  .load("testData/wordFreLine2.txt")
// Split the data into train and test
val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
val train = splits(0)
val test = splits(1)
// specify layers for the neural network:
// input layer of size 4 (features), two intermediate of size 5 and 4
// and output of size 3 (classes)
val layers = Array[Int](5, 5, 4, 6)
// create the trainer and set its parameters
val trainer = new MultilayerPerceptronClassifier()
  .setLayers(layers)
  .setBlockSize(128)
  .setSeed(1234L)
  .setMaxIter(200)
// train the model
val model = trainer.fit(train)

// compute precision on the test set
val result = model.transform(test)
val predictionAndLabels = result.select("prediction", "label")
val evaluator = new MulticlassClassificationEvaluator()
  .setMetricName("precision")
println("Precision:" + evaluator.evaluate(predictionAndLabels))
result.show

}