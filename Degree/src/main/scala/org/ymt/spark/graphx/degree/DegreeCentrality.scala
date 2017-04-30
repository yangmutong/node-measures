package org.ymt.spark.graphx.degree

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by yangmutong on 2017/4/8.
  */
object DegreeCentrality extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Degree Centrality"))
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt

    // graph loader phase
    val graph = makeGraph(inputPath, sc)
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut)
    g.cache()

    val in = inDegreeCentrality(g)
    val out = outDegreeCentrality(g)
    val degree = degreeCentrality(g)

    save(in, outputPath + "/in/vertices")
    save(out, outputPath + "/out/vertices")
    save(degree, outputPath + "/all/vertices")
    sc.stop()
  }

  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext): Graph[Int, Double] = {
    val graph = GraphLoader.edgeListFile(sc, inputPath, true)
    graph.mapEdges(v => v.attr.toDouble)
  }

  def save[VD: ClassTag](vertex: VertexRDD[VD], vertexPath: String): Unit = {
    vertex.saveAsTextFile(vertexPath)
  }
  def inDegreeCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Double] = {
    val length = graph.numVertices
    val s = 1.0 / (length - 1.0)
    graph.inDegrees.mapValues( (vid, value) => value * s)
  }

  def outDegreeCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Double] = {
    val length = graph.numVertices
    val s = 1.0 / (length - 1.0)
    graph.outDegrees.mapValues( (vid, value) => value * s)
  }

  def degreeCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Double] = {
    val length = graph.numVertices
    val s = 1.0 / (length - 1.0)
    graph.degrees.mapValues( (vid, value) => value * s)
  }
}
