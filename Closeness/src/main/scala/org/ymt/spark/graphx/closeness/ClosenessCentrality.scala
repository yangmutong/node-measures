package org.ymt.spark.graphx.closeness

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import scala.language.reflectiveCalls
import scala.language.implicitConversions
import org.ymt.spark.graphx.closeness.ShortestPathsWeighted
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

    val result = run(myGraph)
    sc.stop()
  }

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    val numVertices = graph.numVertices
    Graph(ShortestPathsWeighted.run(graph, graph.vertices.map { vx => vx._1 }.collect())
      .vertices.map {
      vx => (vx._1, {
        val dx = 1.0 / vx._2.values.seq.avg
        if (dx.isNaN | dx.isNegInfinity | dx.isPosInfinity) 0.0 else dx
      })
    }: RDD[(VertexId, Double)], graph.edges)
  }
  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  implicit def iterableWithAvg[T: Numeric](data: Iterable[T]): Object {def avg: Double} = new {
    def avg = average(data)
  }

  def shortestPathLength[VD](graph: Graph[VD, Double], origin: VertexId): Double = {
    val spGraph = graph.mapVertices { (vid, _) =>
      if (vid == origin)
        0.0
      else
        Double.MaxValue
    }

    val initialMessage = Double.MaxValue
    def vertexProgram(vid: VertexId, attr: Double, msg: Double): Double = {
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
