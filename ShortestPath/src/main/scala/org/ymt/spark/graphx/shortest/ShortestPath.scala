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

object ShortestPath extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Shortest Path"))
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(3)

    val graph = GraphLoader.edgeListFile(sc, inputPath).cache()
    val g2 = Graph(graph.vertices, graph.edges.map(e => {
      if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr)
    })).partitionBy(PartitionStrategy.RandomVertexCut)

    val g = Graph(g2.vertices.repartition(numPartitions), g2.edges.repartition(numPartitions))

    val sp = new ShortestPaths



    sc.stop()
  }
}
