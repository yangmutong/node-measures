package org.ymt.spark.graphx.eigenvector

import org.apache.log4j.LogManager

import org.apache.spark.graphx._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.reflect.ClassTag
/**
  * Created by yangmutong on 2017/4/10.
  */

object EigenvectorCentrality extends Serializable{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Eigenvector Centrality")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(EigenvectorCentrality.getClass))
    val sc = new SparkContext(conf)
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt
    val maxIter = args(3).toInt

    // graph loader phase
    val graph = makeGraph(inputPath, sc, numPartitions).persist()
    val result = run(graph, maxIter)

    save(result, outputPath + "/vertices")

    sc.stop()
  }
  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext, numPartitions: Int): Graph[Long, Double] = {
    val graph = GraphLoader.edgeListFile(sc, inputPath, true)
    graph.unpersist()
    val edgesRepartitionRdd = graph.edges.map(
      edge => {
        val pid = PartitionStrategy.EdgePartition2D.getPartition(edge.srcId, edge.dstId, numPartitions)
        (pid, (edge.srcId, edge.dstId))
      }
    ).partitionBy(new HashPartitioner(numPartitions)).map {
      case (pid, (src: Long, dst: Long)) =>
        Edge(src, dst, 1.0)
    }
    edgesRepartitionRdd.unpersist()
    Graph.fromEdges(edgesRepartitionRdd, 0L)
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
  }
  def run[VD](graph: Graph[VD, Double], maxIter: Int = 100): Graph[Double, Double] = {
    eigenvector(graph, maxIter)
  }
  def eigenvector[VD](graph: Graph[VD, Double], maxIter: Int): Graph[Double, Double] = {

    def mergeMsg(msg1: Double, msg2: Double): Double = {
      msg1 + msg2
    }

    def vertexProgram(vid: VertexId, attr: Double, msg: Double): Double = {
      mergeMsg(msg, attr)
    }

    def sendMsg(edgeTriplet: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] = {
      val msg = edgeTriplet.srcAttr * edgeTriplet.attr
      Iterator((edgeTriplet.dstId, msg))
    }

    val count = graph.vertices.count()
    var result = graph.mapVertices((vid, attr) => 1.0 / count)
    var initialGraph: Graph[Double,Double] = result
    var condition = Double.MaxValue
    @transient lazy val log = LogManager.getLogger("myLogger")
    for {i <- 1 to maxIter
        if condition >= count * 0.000001
    } {
      initialGraph.unpersist()
      initialGraph = result
      val tmp = Pregel(initialGraph, 0.0, 1, activeDirection = EdgeDirection.Out)(vertexProgram, sendMsg, mergeMsg)
      val s = math.sqrt(tmp.vertices.map(v => v._2 * v._2).reduce(_+_))
      val normalize = if (s == 0.0) 1.0 else s
      result.unpersist()
      result = tmp.mapVertices((vid, attr) => attr / normalize)
      condition = result.outerJoinVertices[Double, Double](initialGraph.vertices){(vid, leftAttr: Double, rightAttr: Option[Double]) => {
        math.abs(leftAttr - rightAttr.getOrElse(0.0))
      }}.vertices.map(v => v._2).reduce(_+_)
      log.info("Eigenvector centrality iteration " + i)
      log.info("Condition " + condition)
      log.info("Normalize " + normalize)
      log.info("S" + s)
    }
    result
  }
}
