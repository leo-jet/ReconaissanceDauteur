package leo.jet.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils

/**
 * @author cloudera
 */
object DecisionTreeAmazon {
  
  //methode main
  def main(args: Array[String]) {
    
    val sc = new SparkContext(new SparkConf().setAppName("Spark Decision Tres"))
    
    val data = MLUtils.loadLibSVMFile(sc, args(0))
    
    val splits = data.randomSplit(Array(args(1).toDouble, args(2).toDouble))
    
    val (trainingData, testData) = (splits(0), splits(1))
    
    val numClasses = 50
    
    val categoricalFeaturesInfo = Map[Int, Int]()
    
    val impurity ="gini"
    
    val maxDepth = 5
    
    val maxBins = 32 
    
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    
    val labelAndPreds = testData.map{ point =>
      (point.label,  model.predict(point.features))
     }
    
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = "+testErr)
    println("Learned classification tree model:\n"+model.toDebugString)
    
    model.save(sc, "myModel")
    val sameModel = DecisionTreeModel.load(sc, "myModel")
  }
  
}