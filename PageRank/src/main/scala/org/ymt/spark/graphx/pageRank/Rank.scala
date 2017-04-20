package org.ymt.spark.graphx.pageRank

/**
  * Created by yangmutong on 2017/3/26.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._

import scala.reflect.ClassTag

object Rank extends Serializable {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Rank"))
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2)
    val rank = args(3)
    // graph loader phase
    val graph = GraphLoader.edgeListFile(sc, inputPath).cache()
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.map(e => {
        if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr)
      }).repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut)

    val result = g.pageRank(rank)
    save(result, outputPath + "/vertices", outputPath + "/edges")

    sc.stop()
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String, edegePath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
    graph.vertices.saveAsTextFile(edegePath)
  }
}


