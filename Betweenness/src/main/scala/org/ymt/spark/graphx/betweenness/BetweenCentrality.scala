package org.ymt.spark.graphx.betweenness

/**
  * Created by yangmutong on 2017/4/8.
  */

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

import scala.reflect.ClassTag

object BetweenCentrality extends Serializable{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Betweenness Centrality")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext, numPartitions: Int): Graph[Long, Double] = {
    val graph = GraphLoader.edgeListFile(sc, inputPath, true)
    graph.unpersist()
    val edgesRepartitionRdd = graph.edges.map(
      edge => {
        val pid = PartitionStrategy.EdgePartition2D.getPartition(edge.srcId, edge.dstId, numPartitions)
        (pid, (edge.srcId, edge.dstId))
      }
    ).partitionBy(new HashPartitioner(numPartitions)).map {
      case (pid, (src: Long, dst: Long)) =>
        Edge(src, dst, 1.0)
    }
    edgesRepartitionRdd.unpersist()
    Graph.fromEdges(edgesRepartitionRdd, 0L)
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
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
