package org.ymt.spark.graphx.triangle

/**
  * Created by yangmutong on 2017/3/27.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object TriangleCount extends Serializable{
  /**
    * @param args
    *             0 dataset input path
    *             1 result output path
    *             2 num partitions
    * */
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("TriangleCount"))
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt

    // graph loader phase
    val graph = makeGraph(inputPath, sc)
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut)

    val result = g.triangleCount()

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
