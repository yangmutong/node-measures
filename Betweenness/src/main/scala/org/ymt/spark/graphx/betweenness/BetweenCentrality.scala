package org.ymt.spark.graphx.betweenness

/**
  * Created by yangmutong on 2017/4/8.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

object BetweenCentrality extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Intro"))
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2)

    // graph loader phase
    val graph = GraphLoader.edgeListFile(sc, inputPath).cache()
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.map(e => {
        if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr)
      }).repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut)
    val result = KBetweenness.run(g, 10)
    save(result, outputPath + "/vertices", outputPath + "/edges")
    sc.stop()
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
