package org.ymt.spark.graphx.betweenness

/**
  * Created by yangmutong on 2017/4/8.
  */

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

import scala.reflect.ClassTag

object BetweenCentrality extends Serializable{
  /**
    * @author Yang Mutong
    * @param args received from command line
    * @return void
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Betweenness Centrality")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // change Serialize
    conf.registerKryoClasses(Array(BetweenCentrality.getClass, KBetweenness.getClass, ShortestPathsWeighted.getClass))
    val sc = new SparkContext(conf)
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt
    val iter = args(3).toInt

    // graph loader phase
    val graph = makeGraph(inputPath, sc, numPartitions).persist()
    val result = KBetweenness.run(graph, iter)
    save(result, outputPath + "/vertices")
    sc.stop()
  }

  /**
    * @param inputPath input path, HDFS url will be better
    * @param sc SparkContext
    * @param numPartitions number of partitions, square numbers will be better to compute
    * @return Graph[VD, ED]
    */
  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext, numPartitions: Int): Graph[Double, Double] = {
    GraphLoader.edgeListFile(sc, inputPath, canonicalOrientation=true, numEdgePartitions=numPartitions).unpersist()
      .partitionBy(PartitionStrategy.EdgePartition2D).unpersist()
      .mapVertices((vid, attr) => attr.toDouble).unpersist()
      .mapEdges(v => v.attr.toDouble)
  }

  /**
    * @param graph
    * @param vertexPath
    * @tparam VD
    * @tparam ED
    */
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
  }

  /**
    * @deprecated this one is deprecated
    * @param graph input graph
    * @tparam VD
    * @return Graph[List[VertexId], Double]
    */
  def reachableNodes[VD](graph: Graph[VD, Double]): Graph[List[VertexId], Double] = {
    val reachGraph = graph.mapVertices((vid, _) => List[VertexId](vid))
    val initialMessage = List[VertexId]()

    def mergeMessage(msg1: List[VertexId], msg2: List[VertexId]): List[VertexId]  = {
      (msg1 ++ msg2).distinct
    }

    def vertexProgram(vid: VertexId, attr: List[VertexId], msg: List[VertexId]): List[VertexId] = {
      mergeMessage(attr, msg)
    }

    def sendMessage(edgeTriplet: EdgeTriplet[List[VertexId], Double]): Iterator[(VertexId, List[VertexId])] = {
      Iterator((edgeTriplet.srcId, edgeTriplet.dstAttr))
    }
    val result = Pregel(reachGraph, initialMessage, activeDirection = EdgeDirection.In)(vertexProgram, sendMessage, mergeMessage)
    result
  }

}
