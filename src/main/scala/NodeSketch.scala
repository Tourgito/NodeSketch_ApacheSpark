import org.apache.spark.{SparkConf, SparkContext}
import scala.math.{pow, sqrt, log}
import scala.util.Random
import org.apache.spark.sql.functions.{lit,udf, udaf, struct, col, sum, monotonically_increasing_id, row_number,concat,concat_ws,when, split}
import scala.collection.mutable.{Map,WrappedArray}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import scala.io.Source
import java.io.FileWriter
import java.io.BufferedWriter
import org.apache.spark.sql.{SparkSession,DataFrame, Row, Column}
import org.apache.spark.sql.types.{StringType, StructType,StructField, ArrayType, LongType, IntegerType, DoubleType}



 object NodeSketch{

  def main(args: Array[String]) {

    //val sketchVectorSize = 3 // prepei na einai argument tou algorithmou

    val spark = SparkSession
                        .builder()
                        .appName("NodeSketch")
                        .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val k: Int = args(0).toInt
    val sketchDimension: Int = args(1).toInt
    val graph: String = args(2)
    val numberOfPartitions: Int = args(3).toInt


    val e = args(4).toInt     

    val start:Long = System.nanoTime()

    val ns = NodeSketch(spark, graph, 0.2, sketchDimension, numberOfPartitions) 
    ns.execute(k)

    val executionTime: Double = (System.nanoTime() - start).toDouble / 1000000000.0
    println("Xronos ekteleshs: " + executionTime)

    val writer = new BufferedWriter(new FileWriter("./executionsTime_NodeSketch.csv", true))

    if (k < e){
     if (k == 2)
       writer.write(numberOfPartitions.toString + ",")
     writer.write(executionTime.toString() + ",")
         }
    else {
      writer.write(executionTime.toString())
      writer.newLine()
    }
    writer.close()
    //ns.showSketchEmmbendings

  }

  // Class that implements the Nodesketch algorithm
  case class NodeSketch(spark: SparkSession, graph:String, a:Double, sketchDimension: Int, numberOfPartitions:Int) {

    private val numberOfNodes: Int = this.getNumberOfGraphsNodes()

    private val hashFunctionsInputs:List[(Int,Int)]  = (List.range(103244, this.sketchDimension + 103244) zip List.range(53485543, this.sketchDimension + 53485543))

    // The SLA adjejancy matrix of the graph
    // Dataframe with n + 1 columns: n = (number of graph's nodes ), 1 = (nodes' Id) 
    private val SLAMatrix_DF: DataFrame = this.spark.read.schema(getSLAMatrix_DFSchema())
                                                         .csv(graph)
                                                         //.repartition(this.numberOfPartitions)
                                                         //.persist()

    // The nodes' neighboors
    // Dataframe that 2 columns: The first the nodes' Id and the second the nodes' neighboors
    // the neighboors of a node are preserved in a list which contains their indexes in the SLA matrix (e.g [2,4,7])
    private val nodeNeighboors: DataFrame = SLAMatrix_DF.withColumn("Neighboors_str",concat(List.range(0,numberOfNodes)
                                                                                                 .map(x => when(col(x.toString()) === 1, x.toString() + " ").otherwise("")):_*
                                                                                            )
                                                                    )
                                                         .withColumn("Neighboors", split(col("Neighboors_str")," "))
                                                         .select("Id","Neighboors")
                                                         //.repartition(this.numberOfPartitions)
                                                         //.persist()
    

    // The final sketch embeddings of the graph's nodes
    private var sketchEmbenddings: DataFrame = null


    // Implements Algorithm 1 in https://doc.rero.ch/record/327156/files/Yang_et_al_2019.pdf
    def execute(k:Int): Unit = {

      if (k > 2){
        
        this.execute(k-1) // implements line 2 of Algorithm 1

        val columnsToAggregate = SLAMatrix_DF.columns.slice(1,this.numberOfNodes + 1).map(x => sum(x.toString()).as(x.toString()))

        // implements lines 3-6 of Algorithm 1
        this.sketchEmbenddings = this.sketchEmbenddings.join(this.nodeNeighboors,this.sketchEmbenddings("Id") === this.nodeNeighboors("Id"))
                                                       //.repartition(this.numberOfPartitions)
                                                        // the next 4 transformations are calculating Approximate k-order SLA adjejancy Matrix, line 6 Algorithm 1
                                                       .flatMap(x => this.equation6(x, this.numberOfNodes, this.sketchDimension, this.a))(RowEncoder(getDFSchema_AfterEq6()))
                                                       .union(this.SLAMatrix_DF)
                                                       .groupBy("Id")
                                                       .agg(columnsToAggregate.head, columnsToAggregate.tail:_*) // 
                                                       .sort("Id")
                                                       .map(x => this.equation3(x, this.numberOfNodes, this.sketchDimension))(RowEncoder(getDFSchema_AfterEq3())) // Calculating k-order sketch embendding, line 5 of Algorithm 1
      }
      else {
        // Calculating low-order sketch embenddings
        // Implements lines 8-9 of Algorithm 1
        this.sketchEmbenddings = this.SLAMatrix_DF.select(this.SLAMatrix_DF.columns.take(this.numberOfNodes + 1).map(col):_*)
                                                  //.repartition(this.numberOfPartitions)
                                                  .map(nodeVector => this.equation3(nodeVector, this.numberOfNodes, this.sketchDimension))(RowEncoder(getDFSchema_AfterEq3()))

      }
    }

    // A Uniform(0,1) Hash function
    private def hashFunction(a:Int, b:Int, seed:Int): Double = {
      val t = ( (a*seed + b) % 1024 ).toDouble
      if (t == 0.0)
          1.0 / 1024.0
      else 
        t / 1024.0
    }

    private def hashFunction1(index:Int): Double = {
      val value = ( (3248123*index+ 5834205) % 5345344 % 50002 ).toDouble
      if (value == 0.0)
          1.0 / 50002
      else 
        value / 50002
      
    }

    private def hashFunction2(index:Int): Double = {
      val value = ( (76845034*index+ 43267) % 987978643 % 854393 ).toDouble
      if (value == 0.0)
          1.0 / 854393
      else 
        value / 854393
    }

    private def hashFunction3(index:Int): Double = {
      val value = ( (893214322*index+ 8769659) % 765974554 % 5326143 ).toDouble
      if (value == 0.0)
          1.0 / 5326143
      else 
        value / 5326143
    }

    private def hash(i: Int): Double = {
      if (i==1)
        hashFunction1(i)
      else if (i==2)
        hashFunction2(i)
      else
        hashFunction3(i)
    }

    def getSketchEmmbendings: DataFrame = {
      this.sketchEmbenddings
    }

    def showSketchEmmbendings: Unit = {
      this.sketchEmbenddings.show()
    }

    // Returns the number of nodes in the graph
    private def getNumberOfGraphsNodes(): Int = {
        Source.fromFile(this.graph).bufferedReader().readLine().split(",").length - 1
    }

    // Returns the DataFrame schema for the SLA Matrix 
    private def getSLAMatrix_DFSchema() : StructType = {
      StructType(List.range(0,this.numberOfNodes).map(x => StructField(x.toString(), DoubleType, false)).+:(StructField("Id",StringType,false)))
    }

    // Returns the DatFrame schema after equation 3 execution
    private def getDFSchema_AfterEq3() : StructType = {
      StructType(List.range(0,this.sketchDimension)
                      .map(x => StructField(x.toString + "S", StringType, false)).+:(StructField("Id",StringType,false)))
    }

    // Returns the DatFrame schema after equation 6 execution
    private def getDFSchema_AfterEq6() : StructType = {
      StructType(List.range(0,this.numberOfNodes).map(x => StructField(x.toString(), StringType, false)).+:(StructField("Id",StringType,false)))
    } 


    // Implements equation 3 of https://doc.rero.ch/record/327156/files/Yang_et_al_2019.pdf
    private def equation3(nodeVector:Row, numberOfNodes:Int, sketchDimension:Int) : Row = {


        val nodeVector_withoutId = nodeVector.getValuesMap[Double](List.range(0, this.numberOfNodes).map(x => x.toString())).toList.filter(x=> x._2 !=0)

        // The node's sketch vector
        Row.fromSeq(List.range(1, sketchDimension + 1).map(x =>  {
                                                                      // -log * h(i) / Vi applied to each node's vector element
                                                                      val nodeVector_updated = nodeVector_withoutId
                                                                                                             .map(y => {
                                                                                                                        (y._1,(- log(this.hashFunction(this.hashFunctionsInputs(x-1)._1,this.hashFunctionsInputs(x-1)._2, y._1.toInt)) / y._2))
                                                                                                                        //(y._1,(- log(this.hash(x))) / y._2)
                                                                                                                       }
                                                                                                                  )
                                                                      // the argmin of the elements                                             
                                                                      nodeVector_updated.sortBy(x => (x._2,x._1.toInt)).head._1
                                                                  }
                                                            ).+:(nodeVector.getAs[String]("Id")) 
                   )
      }


    // Implements equation 6 of https://doc.rero.ch/record/327156/files/Yang_et_al_2019.pdf
    // Calculates the sketch distribution for a node
    // Returns the node's sketch distribution for each of its neighboor, so they can calculate their k-order Vi vector
    // by aggregating all of their neighboors sketch distributions 
    private def equation6(nodeSketch:Row, numberOfNodes:Int, sketchDimension:Int, a:Double) : WrappedArray[Row] = {


        val nodeId: String = nodeSketch.getAs[String]("Id")
        
        val nodeNeighboors: WrappedArray[String] = nodeSketch.getAs[WrappedArray[String]]("Neighboors").filter(x => x != nodeId) // Must remove a node's self from its neighboors, which is putted because of the SLA matrix

        // Node's sketch vector
        val sketchEmbendding: List[String] = List.range(0, sketchDimension).map(x => nodeSketch.getAs[String](x.toString + "S"))

        // The distribution of the sketch elements
        val sketchElementsDistributions: Map[String, Int] = Map()

        // Calculating the sketch values distributions
        sketchEmbendding.foreach(x => {
                                      if (sketchElementsDistributions.contains(x))
                                          sketchElementsDistributions(x) += 1
                                      else
                                        sketchElementsDistributions(x) = 1
                                      }
                                )
        // Vector that contains the distribution of all elements 
        val distributionVector: List[String] = List.range(0, numberOfNodes).map(x => {
                                                                                      if (sketchElementsDistributions.contains(x.toString())) 
                                                                                       ( (sketchElementsDistributions(x.toString()) / sketchDimension) * a ).toString()
                                                                                      else
                                                                                       "0"
                                                                                      }
                                                                                    )

        nodeNeighboors.slice(0, nodeNeighboors.length - 1).map(neighboor => Row.fromSeq(distributionVector.+:(neighboor)))
        }
      

    }


  } 


  




