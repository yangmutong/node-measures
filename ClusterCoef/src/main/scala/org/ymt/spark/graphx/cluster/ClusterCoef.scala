package org.ymt.spark.graphx.cluster

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Created by yangmutong on 2017/4/10.
  */
object ClusterCoef extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Cluster Coefficient"))
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt

    // graph loader phase
    val graph = makeGraph(inputPath, sc)
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut)

    val result = clusterCoef(g)
    save(result, outputPath + "/vertices")

    sc.stop()
  }

  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext): Graph[Int, Double] = {
    val graph = GraphLoader.edgeListFile(sc, inputPath, true)
    graph.mapEdges(v => v.attr.toDouble)
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
  }
  def clusterCoef[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    val triangleGraph = graph.triangleCount()
    val maxTriangleGraph = graph.degrees.mapValues(d => d * (d - 1) / 2.0)
    val result = triangleGraph.vertices.innerJoin(maxTriangleGraph) {(vid, triangleCount, maxTriangle) => {
      if (maxTriangle == 0) 0 else triangleCount / maxTriangle
    }}
    Graph(result, graph.edges)
  }
}
