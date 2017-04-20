package org.ymt.spark.graphx.betweenness

/**
  * Created by yangmutong on 2017/4/8.
  */

import com.sun.tools.doclint.HtmlTag.Attr
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

import org.ymt.spark.graphx.betweenness.ShortestPathsWeighted

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

object BetweenCentrality extends Serializable{
  type SPMap = Map[VertexId, (Double, List[VertexId], Double)]
  private def makeMap(x: (VertexId, (Double, List[VertexId], Double))*) = Map(x: _*)
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Intro"))
    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val graph = Graph(myVertices, myEdges)
    sc.stop()
  }

//  def betweennessCentrality[VD](graph: Graph[VD, Double]): Graph[Double, Double] = {
//    val reachGraph = reachableNodes[VD](graph)
//    val graphIterator = reachGraph.vertices.toLocalIterator
//    graphIterator.foreach(p => shortestPathLength(reachGraph, p._1))
//
//  }
//
//  def calculateCoef(graph: Graph[(Double, List[VertexId], List[VertexId], Double), Double], origin: VertexId): Double = {
//    val vertexAttr = graph.vertices.filter(p => p._1 == origin).first()._2
//    val reachNodes = vertexAttr._2
//    val preNodes = vertexAttr._3
//    val delta = reachNodes.map(v => (v, 0.0)).toMap
//    val sigma = graph.vertices.filter(p => reachNodes.contains(p._1)).map(s => (s._1, s._2._4)).collect().toMap
//    reachNodes.foreach(vid => {
//      val coeff = (1.0 + delta.get(vid).get) / sigma.get(vid).get
//      preNodes.foreach(v => + )
//    })
//  }
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

  def shortestPathLength(reach: Graph[List[VertexId], Double], origin: VertexId): Graph[SPMap, Double] = {
    val spGraph = reach.mapVertices((vid, attr) => {
      attr.map(v => (v, (Double.MaxValue, List[VertexId](), 0.0))).toMap
    }).mapVertices { (vid, attr) =>
      if (vid == origin){
        attr.map(v => {
          if (v._1 == origin)
            v.
        })
      }
      else
        attr
    }

    val initialMessage = (Double.MaxValue, List[VertexId](), 0.0)
    def vertexProgram(vid: VertexId, attr: (Double, List[VertexId], List[VertexId], Double), msg: (Double, List[VertexId], Double)): (Double, List[VertexId], List[VertexId], Double) = {
      if (attr._1 < msg._1) {
        attr
      } else {
        (msg._1, attr._2, msg._2, msg._3)
      }
    }

    def sendMessage(edgeTriplet: EdgeTriplet[(Double, List[VertexId], List[VertexId], Double), Double]): Iterator[(VertexId, (Double, List[VertexId], Double))] = {
      val newDistance = edgeTriplet.attr + edgeTriplet.srcAttr._1
      if (edgeTriplet.dstAttr._1 > newDistance){
        val preVertices = List[VertexId](edgeTriplet.srcId)
        val throughPaths = edgeTriplet.srcAttr._4
        Iterator((edgeTriplet.dstId, (newDistance, preVertices, throughPaths)))
      } else if (edgeTriplet.dstAttr._1 == newDistance && newDistance != Double.MaxValue) {
        val preVertices = (edgeTriplet.dstAttr._3 :+ edgeTriplet.srcId).distinct
        val throughPaths = edgeTriplet.srcAttr._4
        Iterator((edgeTriplet.dstId, (newDistance, preVertices, throughPaths + 1.0)))
      } else {
        Iterator.empty
      }
    }

    def mergeMessage(msg1: (Double, List[VertexId], Double), msg2: (Double, List[VertexId], Double)): (Double, List[VertexId], Double) = {
      if (msg1._1 < msg2._1) {
        msg1
      } else if (msg1._1 > msg2._1) {
        msg2
      } else {
        (msg1._1, (msg1._2 ++ msg2._2).distinct, msg1._3 + 1.0)
      }
    }
    val newGraph = Pregel(spGraph, initialMessage, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, mergeMessage)

    newGraph
  }



}
