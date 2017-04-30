package org.ymt.spark.graphx.shortest

/**
  * Created by yangmutong on 2017/3/27.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._

import scala.reflect.ClassTag

object ShortestPath extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Shortest Path"))
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt

    val graph = makeGraph(inputPath, sc)
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut).cache()

    val result = org.ymt.spark.graphx.shortest.ShortestPaths.runWithDist(g, g.vertices.map(_._1).collect().toSeq)

    save(result, outputPath + "/vertices", outputPath + "/edges")

    sc.stop()
  }

  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext): Graph[Int, Double] = {
    val graph = GraphLoader.edgeListFile(sc, inputPath, true)
    val edges = sc.textFile(inputPath).map(v => {
      val t = v.split("\t")
      Edge(t(0).toLong, t(1).toLong, t(2).toDouble)
    })
    Graph(graph.vertices, edges)
  }

  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String, edegePath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
    graph.vertices.saveAsTextFile(edegePath)
  }
}
