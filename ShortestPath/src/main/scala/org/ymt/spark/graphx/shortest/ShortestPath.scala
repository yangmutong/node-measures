package org.ymt.spark.graphx.shortest

/**
  * Created by yangmutong on 2017/3/27.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.ymt.spark.graphx.shortest.ShortestPaths

import scala.reflect.ClassTag

object ShortestPath extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Shortest Path"))
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt

    val graph = GraphLoader.edgeListFile(sc, inputPath).cache()
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.map(e => {
        if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr)
      }).repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut)

    val result = ShortestPaths.runWithDist(g, g.vertices.map(v => v._1).collect().toSeq)

    save(result, outputPath + "/vertices", outputPath + "/edges")

    sc.stop()
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String, edegePath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
    graph.vertices.saveAsTextFile(edegePath)
  }
}
