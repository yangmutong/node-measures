package org.ymt.spark.graphx.betweenness


import org.apache.spark.graphx._

import scala.reflect.ClassTag

object ShortestPathsWeighted extends Serializable{
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type SPMap = Map[VertexId, (Double, List[VertexId], Double)]

  private def makeMap(x: (VertexId, (Double, List[VertexId], Double))*) = Map(x: _*)

  private def incrementMap(spmap: SPMap, dist: Double): SPMap = spmap.map {
    case (v, d) => v -> (d._1 + dist, d._2, d._3)
  }
  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => {
        val distance1 = spmap1.getOrElse(k, (Double.MaxValue, List[VertexId](), 0.0))
        val distance2 = spmap2.getOrElse(k, (Double.MaxValue, List[VertexId](), 0.0))
        if (distance1._1 > distance2._1)
          k -> distance2
        else if (distance1._1 < distance2._1)
          k -> distance1
        else
          k -> (distance1._1, (distance1._2 ++ distance2._2).distinct, distance1._3 + 1.0)
      }
    }.toMap

  def runWithDist[VD: ClassTag](graph: Graph[VD, Double], landmarks: Seq[VertexId])
  : Graph[SPMap, Double] = {

    def sendMessage(edge: EdgeTriplet[SPMap, Double]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr, edge.attr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }
    _run(graph, landmarks, sendMessage)
  }

  def _run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                             landmarks: Seq[VertexId],
                             sendMsg: EdgeTriplet[SPMap, ED] => Iterator[(VertexId, SPMap)]): Graph[SPMap, ED] = {
    val spGraph = Graph(graph.collectNeighbors(EdgeDirection.In), graph.edges).mapVertices((vid, attr) => {
      if (landmarks.contains(vid)) {
        if (attr.length > 0)
          makeMap(vid -> (0.0, List[VertexId](attr(0)._1), 1.0))
        else
          makeMap(vid -> (0.0, List[VertexId](), 1.0))
      } else
        makeMap()
    })
    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMsg, addMaps)
  }

}