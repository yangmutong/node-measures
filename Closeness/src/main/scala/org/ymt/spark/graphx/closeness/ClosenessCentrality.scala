package org.ymt.spark.graphx.closeness

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import scala.reflect.ClassTag
/**
  * Created by yangmutong on 2017/4/8.
  */

object ClosenessCentrality extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Intro"))
    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val myGraph = Graph(myVertices, myEdges)

    // 调用
    val count = myGraph.numVertices - 1.0
    val vertices = myGraph.vertices.toLocalIterator
    val result = vertices.map {case (vid, attr) => {
      val length = shortestPathLength(myGraph, vid)
      if (length == 0.0)
        (vid, 0.0)
      else
        (vid, count / length)
      }
    }
    sc.stop()
  }

  // 暂不用
  def closeness[VD](graph: Graph[VD, Double]): Graph[Double, Double] = {
    val count = graph.numVertices - 1.0
    val vertices = graph.vertices.toLocalIterator
    graph.mapVertices( (vid, attr) => count / shortestPathLength(graph, vid))
  }

  def shortestPathLength[VD](graph: Graph[VD, Double], origin: VertexId): Double = {
    val spGraph = graph.mapVertices { (vid, _) =>
      if (vid == origin)
        0.0
      else
        Double.MaxValue
    }

    // val initialMessage = 0.0
    val initialMessage = Double.MaxValue
    def vertexProgram(vid: VertexId, attr: Double, msg: Double): Double = {
//      if (msg == 0.0)
//        attr
//      else
      mergeMessage(attr, msg)
    }

    def sendMessage(edgeTriplet: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] = {
      val newAttr = edgeTriplet.attr + edgeTriplet.srcAttr
      if (edgeTriplet.dstAttr > newAttr)
        Iterator((edgeTriplet.dstId, newAttr))
      else
        Iterator.empty
    }

    def mergeMessage(msg1: Double, msg2: Double): Double = {
      math.min(msg1, msg2)
    }
    val newGraph = Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, mergeMessage)

    newGraph.vertices.map(_._2).filter(_ < Double.MaxValue).sum()
  }
}
