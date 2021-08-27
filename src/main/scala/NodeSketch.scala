import org.apache.spark.{SparkConf, SparkContext}
import scala.math.{pow, sqrt, log}
import scala.util.Random
import org.apache.spark.sql.functions.{lit,udf, udaf, struct, col, sum, monotonically_increasing_id, row_number,concat,concat_ws,when, split}
import scala.collection.mutable.{Map,WrappedArray}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import scala.io.Source
import org.apache.spark.sql.{SparkSession,DataFrame, Row, Column}
import org.apache.spark.sql.types.{StringType, StructType,StructField, ArrayType, LongType, IntegerType, DoubleType}



 object NodeSketch{

  def main(args: Array[String]) {

    val sketchVectorSize = 3 // prepei na einai argument tou algorithmou

    val spark = SparkSession
                        .builder()
                        .appName("NodeSketch")
                        .getOrCreate()

    val ns = NodeSketch(spark, "./graph.csv", 0.2, sketchVectorSize) 
    
    ns.execute(2)
    ns.showSketchEmmbendings

  }

  // Class that implements the Nodesketch algorithm
  case class NodeSketch(spark: SparkSession,path:String, a:Double, sketchVectorSize: Int) {

    private val numberOfNodes: Int = this.getNumberOfGraphsNodes()

    // The SLA adjejancy matrix of the graph
    // Dataframe with n + 1 columns: n = (number of graph's nodes ), 1 = (nodes' Id) 
    private val SLAMatrix_DF: DataFrame = this.spark.read.schema(getSLAMatrix_DFSchema())
                                                         .csv("./graph.csv")
                                                         .persist()

    // The nodes' neighboors
    // Dataframe that 2 columns: The first the nodes' Id and the second the nodes' neighboors
    // the neighboors of a node are preserved in a list which contains their indexes in the SLA matrix (e.g [2,4,7])
    private val nodeNeighboors: DataFrame = SLAMatrix_DF.withColumn("Neighboors_str",concat(List.range(0,numberOfNodes)
                                                                                                 .map(x => when(col(x.toString()) === 1, x.toString() + " ").otherwise("")):_*
                                                                                            )
                                                                    )
                                                         .withColumn("Neighboors", split(col("Neighboors_str")," "))
                                                         .select("Id","Neighboors")
                                                         .persist()
    

    // The final sketch embeddings of the graph's nodes
    private var sketchEmbenddings: DataFrame = null


    // Implements Algorithm 1 in https://doc.rero.ch/record/327156/files/Yang_et_al_2019.pdf
    def execute(k:Int): Unit = {

      if (k > 2){
        
        this.execute(k-1) // implements line 2 of Algorithm 1

        val columnsToAggregate = SLAMatrix_DF.columns.slice(1,this.numberOfNodes + 1).map(x => sum(x.toString()).as(x.toString()))

        // implements lines 3-6 of Algorithm 1
        this.sketchEmbenddings = this.sketchEmbenddings.join(this.nodeNeighboors,this.sketchEmbenddings("Id") === this.nodeNeighboors("Id"))
                                                        // the next 4 transformations are calculating Approximate k-order SLA adjejancy Matrix, line 6 Algorithm 1
                                                       .flatMap(x => this.equation6(x, this.numberOfNodes, this.sketchVectorSize, this.a))(RowEncoder(getDFSchema_AfterEq6()))
                                                       .union(this.SLAMatrix_DF)
                                                       .groupBy("Id")
                                                       .agg(columnsToAggregate.head, columnsToAggregate.tail:_*) // 
                                                       .sort("Id")
                                                       .map(x => this.equation3(x, this.numberOfNodes, this.sketchVectorSize))(RowEncoder(getDFSchema_AfterEq3())) // Calculating k-order sketch embendding, line 5 of Algorithm 1
      }
      else {
        // Calculating low-order sketch embenddings
        // Implements lines 8-9 of Algorithm 1
        this.sketchEmbenddings = this.SLAMatrix_DF.select(this.SLAMatrix_DF.columns.take(this.numberOfNodes + 1).map(col):_*)
                                                  .map(nodeVector => this.equation3(nodeVector, this.numberOfNodes, this.sketchVectorSize))(RowEncoder(getDFSchema_AfterEq3()))
      }
    }


    def getSketchEmmbendings: DataFrame = {
      this.sketchEmbenddings
    }

    def showSketchEmmbendings: Unit = {
      this.sketchEmbenddings.show()
    }

    // Returns the number of nodes in the graph
    private def getNumberOfGraphsNodes(): Int = {
        Source.fromFile(path).bufferedReader().readLine().split(",").length - 1
    }

    // Returns the DataFrame schema for the SLA Matrix 
    private def getSLAMatrix_DFSchema() : StructType = {
      StructType(List.range(0,this.numberOfNodes).map(x => StructField(x.toString(), DoubleType, false)).+:(StructField("Id",StringType,false)))
    }

    // Returns the DatFrame schema after equation 3 execution
    private def getDFSchema_AfterEq3() : StructType = {
      StructType(List.range(0,this.sketchVectorSize)
                      .map(x => StructField(x.toString + "S", StringType, false)).+:(StructField("Id",StringType,false)))
    }

    // Returns the DatFrame schema after equation 6 execution
    private def getDFSchema_AfterEq6() : StructType = {
      StructType(List.range(0,this.numberOfNodes).map(x => StructField(x.toString(), StringType, false)).+:(StructField("Id",StringType,false)))
    } 


      // Implements equation 3 of https://doc.rero.ch/record/327156/files/Yang_et_al_2019.pdf
    private def equation3(nodeVector:Row, numberOfNodes:Int, sketchVectorSize:Int) : Row = {


        val nodeVector_withoutId: Seq[Double] = nodeVector.toSeq.slice(1,numberOfNodes + 1).map(x => x.toString().toDouble)

        // The node's sketch vector
        Row.fromSeq(List.range(1, sketchVectorSize + 2).map(x =>  {
                                                                    if (x != 1) { // The first element of a node's sketch is its Id

                                                                      // -log * h(i) / Vi applied to each node's vector element
                                                                      val nodeVector_updated = nodeVector_withoutId
                                                                                                             .map(y => if (y != 0)
                                                                                                                        - log(this.uniformValueGenerator(x -1)) / y
                                                                                                                        //- y
                                                                                                                       else
                                                                                                                        100.0 
                                                                                                                  )
                                                                      // the arggmin of the elements                                             
                                                                      nodeVector_updated.indexOf(nodeVector_updated.min).toString()
                                                                    }
                                                                    else
                                                                      nodeVector.getAs[String]("Id") // node's Id
                                                                  }
                                                            ) 
                   )
      }

      // Implements equation 6 of https://doc.rero.ch/record/327156/files/Yang_et_al_2019.pdf
      // Calculates the sketch distribution for a node
      // Returns the node's sketch distribution for each of its neighboor, so they can calculate their k-order Vi vector
      // by aggregating all of their neighboors sketch distributions 
    private def equation6(nodeSketch:Row, numberOfNodes:Int, sketchVectorSize:Int, a:Double) : WrappedArray[Row] = {


        val nodeId: String = nodeSketch.getAs[String]("Id")
        
        val nodeNeighboors: WrappedArray[String] = nodeSketch.getAs[WrappedArray[String]]("Neighboors").filter(x => x != nodeId) // Must remove a node's self from its neighboors, which is putted because of the SLA matrix

        // Node's sketch vector
        val sketchEmbendding: List[String] = List.range(0, sketchVectorSize).map(x => nodeSketch.getAs[String](x.toString + "S"))

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
                                                                                       ( (sketchElementsDistributions(x.toString()) / sketchVectorSize) * a ).toString()
                                                                                      else
                                                                                       "0"
                                                                                      }
                                                                                    )

        nodeNeighboors.slice(0, nodeNeighboors.length - 1).map(neighboor => Row.fromSeq(distributionVector.+:(neighboor)))
        }
      
      // Implements the hash function of equation 3
    private def uniformValueGenerator(seed: Int): Double = {
        new Random(seed).nextDouble()
      }  

    }
  } 


  




