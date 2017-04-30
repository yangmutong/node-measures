package org.ymt.spark.graphx.betweenness

/**
  * Created by yangmutong on 2017/4/8.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

import scala.reflect.ClassTag

object BetweenCentrality extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Betweenness Centrality"))
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt
    val iter = args(3).toInt

    // graph loader phase
    val graph = makeGraph(inputPath, sc)
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut)

    val result = KBetweenness.run(g, iter)
    save(result, outputPath + "/vertices", outputPath + "/edges")
    sc.stop()
  }
  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext): Graph[Int, Double] = {
    val graph = GraphLoader.edgeListFile(sc, inputPath, true)
    val edges = sc.textFile(inputPath).map(v => {
      val t = v.split("\t")
      Edge(t(0).toLong, t(1).toLong, t(2).toDouble)
    })
    Graph(graph.vertices, edges)
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String, edegePath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
    graph.vertices.saveAsTextFile(edegePath)
  }

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
