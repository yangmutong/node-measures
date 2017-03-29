package org.ymt.spark.graphx.triangle

/**
  * Created by yangmutong on 2017/3/27.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object TriangleCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("TriangleCount"))
    val baseUrl = "hdfs://master:8020/user/ymt/"
    val graph = GraphLoader.edgeListFile(sc, baseUrl + "slashdot/soc-Slashdot0811.txt").cache()
    val g2 = Graph(graph.vertices, graph.edges.map(e => {
      if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr)
    })).partitionBy(PartitionStrategy.RandomVertexCut)

    (0 to 6).map(i => g2.subgraph(vpred =
      (vid, _) => vid >= i * 10000 && vid < (i + 1) * 10000
    ).triangleCount().vertices.map(_._2).reduce(_ + _))
  }
}
