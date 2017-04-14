package org.ymt.spark.graphx.degree

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

/**
  * Created by yangmutong on 2017/4/8.
  */
object DegreeCentrality extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("DegreeCentrality"))

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))
    val myGraph = Graph(myVertices, myEdges)

    val in = inDegreeCentrality(myGraph)
    val out = outDegreeCentrality(myGraph)
    val degree = degreeCentrality(myGraph)
  }
  def inDegreeCentrality(graph: Graph[_, _]): Graph[Double, _] = {
    val length = graph.numVertices
    val s = 1.0 / (length - 1.0)
    val vertices = graph.inDegrees.mapValues( (vid, value) => value * s)
    Graph(vertices, graph.edges)
  }

  def outDegreeCentrality(graph: Graph[_, _]): Graph[Double, _] = {
    val length = graph.numVertices
    val s = 1.0 / (length - 1.0)
    val vertices = graph.outDegrees.mapValues( (vid, value) => value * s)
    Graph(vertices, graph.edges)
  }

  def degreeCentrality(graph: Graph[_, _]): Graph[Double, _] = {
    val length = graph.numVertices
    val s = 1.0 / (length - 1.0)
    val vertices = graph.degrees.mapValues( (vid, value) => value * s)
    Graph(vertices, graph.edges)
  }
}
