package org.ymt.spark.graphx.eigenvector

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangmutong on 2017/4/10.
  */

object EigenvectorCentrality extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Shortest Path"))
    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val graph = Graph(myVertices, myEdges)
    sc.stop()
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
    val initialMessage = 0.0
    var normalize = 0.0
    var condition = Double.MaxValue
    for (i <- 1 to maxIter) {
      initialGraph = result
      val tmp = Pregel(initialGraph, initialMessage, 1, activeDirection = EdgeDirection.Out)(vertexProgram, sendMsg, mergeMsg)
      normalize = math.sqrt(tmp.vertices.map(v => v._2 * v._2).reduce(_+_))
      result = tmp.mapVertices((vid, attr) => attr / normalize)
      condition = result.outerJoinVertices[Double, Double](initialGraph.vertices){(vid, leftAttr: Double, rightAttr: Option[Double]) => {
        leftAttr - rightAttr.getOrElse(0.0)
      }}.vertices.map(v => v._2).reduce(_+_)
      if (condition < count * 0.000001)
        return result
    }
    result
  }
}
