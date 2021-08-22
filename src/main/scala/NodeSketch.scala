import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import scala.math.{pow, sqrt, log}
import java.io.File
import java.io.PrintWriter
import java.io.FileOutputStream
import org.apache.spark.sql.functions.{lit,udf, udaf, struct, col, sum, monotonically_increasing_id, row_number}
import org.apache.spark.sql.functions
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ListMap
import scala.collection.mutable.WrappedArray

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import scala.io.Source


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType,StructField, ArrayType, LongType, IntegerType, DoubleType}
import org.apache.spark.sql.expressions.{Aggregator}



object NodeSketch{

 def main(args: Array[String]) {

  val numberOfNodes = Source.fromFile("./graph.csv").bufferedReader().readLine().split(",").length - 1
  val sketchVectorSize = 3 // prepei na einai argument tou algorithmou

  val r = NodeSketchAlgorithm("./graph.csv", 0.2, numberOfNodes, sketchVectorSize) 
    
  r.executeAlgorithm(4)
  r.showSketchEmmbendings


 }

 


  case class NodeSketchAlgorithm(path:String, a:Double, numberOfNodes: Int, sketchVectorSize: Int) {

    private val spark = SparkSession
                        .builder()
                        .appName("NodeSketch")
                        .getOrCreate()

    private val SLAMatrix_DF = spark.read.schema(generateSLAMatrix_DFSchema())
                                 .csv("./graph.csv")
                                 .persist()

    private var sketchEmmbendings: DataFrame = null

    def executeAlgorithm(k:Int): Unit = {

      if (k > 2){
        
      this.executeAlgorithm(k-1)

      this.sketchEmmbendings = this.sketchEmmbendings.join(this.SLAMatrix_DF, "Id")            

      val columnList = List.range(1,numberOfNodes +1).map(x => sum(x.toString()).as(x.toString()))

      this.sketchEmmbendings = this.sketchEmmbendings.flatMap(x => NodeSketchAlgorithm.equation6(x, this.numberOfNodes, this.sketchVectorSize, this.a))(RowEncoder(getDFSchema_AfterEq6()))
                                          .groupBy("Id")
                                          .agg(columnList.head, columnList.tail:_*)
                                          .sort("Id") // Den 8a xreiastei
                                          .map(x => NodeSketchAlgorithm.equation3(x, this.numberOfNodes, this.sketchVectorSize))(RowEncoder(generateSketch_DFSchema()))
      }
      else {
        this.sketchEmmbendings = this.SLAMatrix_DF.map(x => NodeSketchAlgorithm.equation3(x, this.numberOfNodes, this.sketchVectorSize))(RowEncoder(generateSketch_DFSchema()))
      }
    }

    def getSketchEmmbendings: DataFrame = {
      this.sketchEmmbendings
    }

    def showSketchEmmbendings: Unit = {
      this.sketchEmmbendings.show()
    }

    private def generateSLAMatrix_DFSchema() : StructType = {
      StructType(List.range(1,this.numberOfNodes + 1).map(x => StructField(x.toString(), DoubleType, false)).+:(StructField("Id",StringType,false)))
    }

    private def generateSketch_DFSchema() : StructType = {
      StructType(List.range(1,this.sketchVectorSize + 1).map(x => StructField(x.toString + "S", StringType, false)).+:(StructField("Id",StringType,false)))
    }

    private def getDFSchema_AfterEq6() : StructType = {
      val a = List.range(1,this.numberOfNodes + 1).map(x => StructField(x.toString(), StringType, false)).+:(StructField("Id",StringType,false))
      StructType(a)
    }

    object NodeSketchAlgorithm {

      def equation3(nodeVector:Row, numberOfNodes:Int, sketchVectorSize:Int) : Row = {

        val nodeVector_withoutId: List[Double] = nodeVector.getValuesMap[Double](List.range(1, numberOfNodes + 1).map(x=> x.toString())).toList
                                                                                                                       .sortBy(x => x._1)
                                                                                                                       .map(x => x._2)

        Row.fromSeq(List.range(1, sketchVectorSize + 1).map(x =>  { 
                                                                    val e : List[Double] = nodeVector_withoutId.map( x => - x ) //- log(2.0) / extractDouble(x)
                                                                    e.indexOf(e.min).toString()
                                                                  }).+:(nodeVector.getAs[String]("Id"))  
                                                                    )

                                                              
      }

      def equation6(nodeSketch:Row, numberOfNodes:Int, sketchVectorSize:Int, a:Double) : List[Row] = {


        val nodeId: String = nodeSketch.getAs[String]("Id") 

        //val nodeVector = List.range(1, this.numberOfNodes + 1).map(x => nodeSketch.getAs[String](x.toString))
        val nodeVector: List[(String, Double)] = nodeSketch.getValuesMap[Double](List.range(1, numberOfNodes +1).map(x => x.toString())).toList.sortBy(x => x._1)

        val nodeVector_withId: List[String] = nodeVector.map(x => x._2.toString()).+:(nodeId)

 
        // sketch vector of node
        val sketch: List[String] = List.range(1, sketchVectorSize + 1).map(x => nodeSketch.getAs[String](x.toString + "S"))

        val sketchDistributions: Map[String, Int] = Map()
        for (nodeId <- 0 to numberOfNodes - 1) { 
          sketchDistributions(nodeId.toString()) = 0
        }
        sketch.foreach(x => sketchDistributions(x.toString()) += 1)

        
        val node_sketchDistribution: List[String] = List.range(0, numberOfNodes).map(x => ((sketchDistributions(x.toString()) / sketchVectorSize) * a).toString())


        val b = ListBuffer[Row]()
        nodeVector.foreach(x => { // endexomenos den xreiazetai Listbuffer, apla anti foreach kanw map
                          if (x._2 == 1)
                            if ( (x._1.toInt - 1).toString != nodeId )
                              b += Row.fromSeq(node_sketchDistribution.+:((x._1.toInt - 1).toString))
                            else
                              b += Row.fromSeq(nodeVector_withId)
        })
        b.toList      

        }

      def extractDouble(expectedNumber: Any): Double = expectedNumber match {
        case i: Int => i.toDouble
        case l: Long => l.toDouble
        case d: Double => d
        //case s: String => Try(s.trim.toDouble).toOption
        case _ => 1 // Prepei na kanw kalutero validation
      }

      def getColumnsName(): List[String] ={
        List.range(1, numberOfNodes + 1).map(x => if (x==1)
                                                    "Id"
                                                  else 
                                                    x.toString())
      }

    }


  /*
  object MergedDistribution extends Aggregator[List[String],List[String],List[String]] {

  def zero: List[String] = List("0","0","0")
    
  def reduce(buffer: List[String], rr: List[String]): List[String] = {
    val r = buffer
    val b  = (r zip rr).map(x => (x._1.toDouble + x._2.toDouble).toString)
    b
  }

  def merge(b1: List[String], b2: List[String]): List[String] = {
    val t = (b1 zip b2).map(x => (x._1.toDouble + x._2.toDouble).toString)
    t
  }

  def finish(reduction: List[String]): List[String] = reduction

  def bufferEncoder: Encoder[List[String]] = Encoders.product

  def outputEncoder: Encoder[List[String]] = Encoders.product
 }
 */


  }

  

}



