package leo.jet.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import Array._
import collection.breakOut
import akka.dispatch.Foreach
import java.util.Scanner
import java.io.PrintWriter
import java.io.File
import java.io.Writer
import scala.io.Source

/**
 * @author cloudera
 */
object ExtractMatrix {
  
  //methode main
  def main(args: Array[String]) {
    
    //creation du spark context
     val sc = new SparkContext(new SparkConf().setAppName("Spark extract matrix"))
    
    //fonction pour la validation de l'extraction d'une line 
    //quand la fonction retourne true la ligne sera extraite sinon elle n'est pas extraite
    //pour qu'une ligne soit extraite il faut qu'elle soit non vide et ne contienne pas de signe @
    def extractData(line: String): Boolean={
       line.indexOf("@") == -1 && !line.isEmpty()
     }
     
     def extractClass(line: String): Boolean={
       line.indexOf("@attribute class {") > -1
     }
    
    //extraction de la matrice
    val matrixtf = sc.textFile(args(0)).filter(extractData)
    //sauvegarde du fichier qui contient la matrix
    matrixtf.saveAsTextFile("target/filters/matrixTF")
    
    //extraction des classes
    val classes = sc.textFile(args(0)).filter(extractClass).map(l => l.substring(l.indexOf("{")+1, l.indexOf("}"))).flatMap(_.split(","))
    val classPos = classes.zipWithIndex
    val classPosMap = classPos.collectAsMap
    classPos.saveAsTextFile("target/filters/classes")
    
    //traitement de la matrix de donnees
    
    //fonction qui transforme en modifiant la classe par le numéro correspondant 
    def transformerLigne(line: String): String ={
      val l = line.substring(line.lastIndexOf(",")+1, line.length())
      val l2 = classPosMap(l).toString() + "," + line.substring(0, line.lastIndexOf(","))
      
      return l2
    }

    //fonction qui enlève la colonne des classes
    def transformerLigne2(line: String): String ={
      val l = line.substring(line.lastIndexOf(",")+1, line.length())
      val l1 = line.substring(0, line.lastIndexOf(","))
      val l2 = l1.split(",")
      val n = range(1,10000)
      var i = 0
      val writer = new PrintWriter(new File("test.txt" ))
      writer.write(classPosMap(l).toString().concat(" "))
      for(i <- 0 to 9999){
        if(!l2(i).equals('0'.toString())){
         writer.write((i+1).toString()+":"+l2(i)+" ")
        }
      }
      writer.close()
      return Source.fromFile("test.txt").getLines().mkString
    }
    
    matrixtf.map(l => transformerLigne(l)).saveAsTextFile("target/filters/matrixTF1")
    matrixtf.map(l => transformerLigne2(l)).saveAsTextFile("target/filters/matrixTF2")
  }
  
}