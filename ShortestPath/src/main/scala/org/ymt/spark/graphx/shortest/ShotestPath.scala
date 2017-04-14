package org.ymt.spark.graphx.shortest

/**
  * Created by yangmutong on 2017/3/27.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object ShotestPath extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Shortest Path"))
    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val myGraph = Graph(myVertices, myEdges)
    dijkstra(myGraph, 1L).vertices.map(_._2).collect
    sc.stop()
  }
  def dijkstra[VD](g: Graph[VD, Double], origin: VertexId) = {
    var g2 = g.mapVertices((vid, vd) => (false, if (vid == origin) 0 else Double.MaxValue))

    for (i <- 1L to g.vertices.count - 1) {
      val currentVertexId = g2.vertices.filter(!_._2._1)
          .fold((0L, (false, Double.MaxValue)))((a,b) => if (a._2._2 < b._2._2) a else b)._1

      val newDistances = g2.aggregateMessages[Double](
        ctx => if (ctx.srcId == currentVertexId) ctx.sendToDst(ctx.srcAttr._2 + ctx.attr),
        (a,b) => math.min(a,b))

      g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) =>
        (vd._1 || vid == currentVertexId, math.min(vd._2, newSum.getOrElse(Double.MaxValue))))
    }

    g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
      (vd, dist.getOrElse((false, Double.MaxValue))._2))
  }
  def dijkstraTrace[VD](g: Graph[VD, Double], origin: VertexId) = {
    var g2 = g.mapVertices(
      (vid, vd) => (false, if (vid == origin) 0 else Double.MaxValue,
        List[VertexId]()))

    for (i <- 1L to g.vertices.count - 1) {
      val currentVertexId =
        g2.vertices.filter(!_._2._1)
          .fold((0L,(false, Double.MaxValue, List[VertexId]())))((a, b) =>
            if (a._2._2 < b._2._2) a else b)
          ._1

      val newDistances = g2.aggregateMessages[(Double, List[VertexId])](
        ctx => if (ctx.srcId == currentVertexId)
          ctx.sendToDst((ctx.srcAttr._2 + ctx.attr,
            ctx.srcAttr._3 :+ ctx.srcId)),
        (a, b) => if (a._1 < b._1) a else b)

      g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) => {
        val newSumVal = newSum.getOrElse((Double.MaxValue, List[VertexId]()))
        (vd._1 || vid == currentVertexId,
          math.min(vd._2, newSumVal._1),
          if (vd._2 < newSumVal._1) vd._3 else newSumVal._2)})
    }
    g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
      (vd, dist.getOrElse((false, Double.MaxValue,List[VertexId]())).productIterator.toList.tail))
  }
}
