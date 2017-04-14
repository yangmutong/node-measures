package org.ymt.spark.graphx.cluster

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangmutong on 2017/4/10.
  */
object ClusterCoef extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Shortest Path"))
    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val myGraph = Graph(myVertices, myEdges)
    val result = clusterCoef(myGraph)
    sc.stop()
  }
  def clusterCoef[VD, ED](graph: Graph[VD, ED]): Graph[Double, ED] = {
    val triangleGraph = graph.triangleCount()
    val maxTriangleGraph = graph.degrees.mapValues(d => d * (d - 1) / 2.0)
    val result = triangleGraph.vertices.innerJoin(maxTriangleGraph) {(vid, triangleCount, maxTriangle) => {
      if (maxTriangle == 0) 0 else triangleCount / maxTriangle
    }}
    Graph(result, graph.edges)
  }
}
