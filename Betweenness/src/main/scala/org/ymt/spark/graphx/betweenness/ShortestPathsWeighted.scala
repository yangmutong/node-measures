package org.ymt.spark.graphx.betweenness

import org.apache.spark.graphx._

import scala.reflect.ClassTag

object ShortestPathsWeighted extends Serializable{
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, Double]

  private def makeMap(x: (VertexId, Double)*) = Map(x: _*)

  private def incrementMap(spmap: SPMap): SPMap = spmap.map { case (v, d) => v -> (d + 1) }

  private def incrementMap(spmap: SPMap, dist: Double): SPMap = spmap.map {
    case (v, d) => v -> (d + dist)
  }
  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Double.MaxValue), spmap2.getOrElse(k, Double.MaxValue))
    }.toMap

  def run[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId]): Graph[SPMap, ED] = {
    def sendMessage(edge: EdgeTriplet[SPMap, ED]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }
    _run(graph, landmarks, sendMessage)
  }

  def runWithDist[VD: ClassTag](graph: Graph[VD, Double], landmarks: Seq[VertexId])
  : Graph[SPMap, Double] = {

    def sendMessage(edge: EdgeTriplet[SPMap, Double]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr, edge.attr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }
    _run(graph, landmarks, sendMessage)
  }

  def _run[VD, ED: ClassTag](graph: Graph[VD, ED],
                             landmarks: Seq[VertexId],
                             sendMsg: EdgeTriplet[SPMap, ED] => Iterator[(VertexId, SPMap)]): Graph[SPMap, ED] = {

    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> 0) else makeMap()
    }

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMsg, addMaps)
  }

}
