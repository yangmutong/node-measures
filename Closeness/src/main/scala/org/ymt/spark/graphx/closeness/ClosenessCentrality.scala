package org.ymt.spark.graphx.closeness

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD

import scala.language.reflectiveCalls
import scala.language.implicitConversions
/**
  * Created by yangmutong on 2017/4/8.
  */

object ClosenessCentrality extends Serializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Closeness Centrality")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(ShortestPathsWeighted.getClass, classOf[ShortestPathsWeighted.SPMap], ClosenessCentrality.getClass))
    val sc = new SparkContext(conf)
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt

    // graph loader phase
    val graph = makeGraph(inputPath, sc, numPartitions).persist()
    val result = run(graph)
    // val result = sc.parallelize(graph.vertices.map(_._1).collect().map(id => shortestPathLength(graph, id)))
    save(result, outputPath + "/vertices")
    sc.stop()
  }


  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext, numPartitions: Int): Graph[Double, Double] = {
    GraphLoader.edgeListFile(sc, inputPath, canonicalOrientation=true, numEdgePartitions=numPartitions).unpersist()
      .partitionBy(PartitionStrategy.EdgePartition2D).unpersist()
      .mapVertices((vid, attr) => attr.toDouble).unpersist()
      .mapEdges(v => v.attr.toDouble)
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
  }

  def run[VD: ClassTag](graph: Graph[VD, Double]): Graph[Double, Double] = {
    val numVertices = graph.numVertices
    Graph(ShortestPathsWeighted.runWithDist(graph, graph.vertices.map(_._1).collect())
      .vertices.map {
      vx => (vx._1, {
        val dx = 1.0 / vx._2.values.seq.avg
        if (dx.isNaN | dx.isNegInfinity | dx.isPosInfinity) 0.0 else dx
      })
    }: RDD[(VertexId, Double)], graph.edges)
     // 性能太差
  }
  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  implicit def iterableWithAvg[T: Numeric](data: Iterable[T]): Object {def avg: Double} = new {
    def avg = average(data)
  }

  def shortestPathLength[VD](graph: Graph[VD, Double], origin: VertexId): Tuple2[Long, Double] = {
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
    val result = Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, mergeMessage).vertices.map(_._2).filter(_ < Double.MaxValue)

    (origin, result.sum() / result.count())
  }
}
