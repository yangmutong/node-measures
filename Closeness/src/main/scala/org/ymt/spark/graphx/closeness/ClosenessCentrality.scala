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
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2)

    // graph loader phase
    val graph = GraphLoader.edgeListFile(sc, inputPath).cache()
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.map(e => {
        if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr)
      }).repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut)
    val result = run(g)
    save(result, outputPath + "/vertices", outputPath + "/edges")
    sc.stop()
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String, edegePath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
    graph.vertices.saveAsTextFile(edegePath)
  }

  def run[VD: ClassTag](graph: Graph[VD, Double]): Graph[Double, Double] = {
    // val numVertices = graph.numVertices
    Graph(ShortestPathsWeighted.runWithDist(graph, graph.vertices.map { vx => vx._1 }.collect())
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

//  def shortestPathLength[VD](graph: Graph[VD, Double], origin: VertexId): Double = {
//    val spGraph = graph.mapVertices { (vid, _) =>
//      if (vid == origin)
//        0.0
//      else
//        Double.MaxValue
//    }
//
//    val initialMessage = Double.MaxValue
//    def vertexProgram(vid: VertexId, attr: Double, msg: Double): Double = {
//      mergeMessage(attr, msg)
//    }
//
//    def sendMessage(edgeTriplet: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] = {
//      val newAttr = edgeTriplet.attr + edgeTriplet.srcAttr
//      if (edgeTriplet.dstAttr > newAttr)
//        Iterator((edgeTriplet.dstId, newAttr))
//      else
//        Iterator.empty
//    }
//
//    def mergeMessage(msg1: Double, msg2: Double): Double = {
//      math.min(msg1, msg2)
//    }
//    val newGraph = Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, mergeMessage)
//
//    newGraph.vertices.map(_._2).filter(_ < Double.MaxValue).sum()
//  }
}
