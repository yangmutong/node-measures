package org.ymt.spark.graphx.eigenvector

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag
/**
  * Created by yangmutong on 2017/4/10.
  */

object EigenvectorCentrality extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Eigenvector Centrality"))
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt

    // graph loader phase
    val graph = makeGraph(inputPath, sc)
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut)

    val result = run(g)

    save(result, outputPath + "/vertices")

    sc.stop()
  }
  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext): Graph[Int, Double] = {
    val graph = GraphLoader.edgeListFile(sc, inputPath, true)
    graph.mapEdges(v => v.attr.toDouble)
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
  }
  def run[VD](graph: Graph[VD, Double], maxIter: Int = 100): Graph[Double, Double] = {
    eigenvector(graph, maxIter)
  }
  def eigenvector[VD](graph: Graph[VD, Double], maxIter: Int): Graph[Double, Double] = {

    def mergeMsg(msg1: Double, msg2: Double): Double = {
      msg1 + msg2
    }

    def vertexProgram(vid: VertexId, attr: Double, msg: Double): Double = {
      mergeMsg(msg, attr)
    }

    def sendMsg(edgeTriplet: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] = {
      val msg = edgeTriplet.srcAttr * edgeTriplet.attr
      Iterator((edgeTriplet.dstId, msg))
    }

    val count = graph.vertices.count()
    var result = graph.mapVertices((vid, attr) => 1.0 / count)
    var initialGraph: Graph[Double,Double] = result
    var condition = Double.MaxValue
    for {i <- 1 to 20
        if condition >= count * 0.000001
    } {
      initialGraph = result
      val tmp = Pregel(initialGraph, 0.0, 1, activeDirection = EdgeDirection.Out)(vertexProgram, sendMsg, mergeMsg)
      val normalize = math.sqrt(tmp.vertices.map(v => v._2 * v._2).reduce(_+_))
      result = tmp.mapVertices((vid, attr) => attr / normalize)
      condition = result.outerJoinVertices[Double, Double](initialGraph.vertices){(vid, leftAttr: Double, rightAttr: Option[Double]) => {
        math.abs(leftAttr - rightAttr.getOrElse(0.0))
      }}.vertices.map(v => v._2).reduce(_+_)
    }
    result
  }
}
