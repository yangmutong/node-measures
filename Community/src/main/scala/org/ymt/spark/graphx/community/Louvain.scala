package org.ymt.spark.graphx.community
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by yangmutong on 2017/4/14.
  */
object Louvain extends Serializable{

  var qValues = Array[(Int, Double)]()

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Louvain"))

    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2)

    // graph loader phase
    val graph = GraphLoader.edgeListFile(sc, inputPath).cache()
    val g = Graph(graph.vertices.repartition(numPartitions),
      graph.edges.map(e => {
        if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr)
      }).repartition(numPartitions)).partitionBy(PartitionStrategy.RandomVertexCut)

    // computation phase
    val louvainCore = new LouvainCore
    var louvainGraph = louvainCore.createLouvainGraph(graph.mapEdges(v => v.attr.toLong))

    var compressionLevel = -1 // number of times the graph has been compressed
    var q_modularityValue = -1.0 // current modularity value
    var halt = false
    do {
      compressionLevel += 1
      println(s"\nStarting Louvain level $compressionLevel")

      // label each vertex with its best community choice at this level of compression
      val (currentQModularityValue, currentGraph, numberOfPasses) = louvainCore.louvain(sc, louvainGraph )
      louvainGraph.unpersistVertices(blocking = false)
      louvainGraph = currentGraph

      saveLevel(sc, compressionLevel, currentQModularityValue, louvainGraph, outputPath)

      // If modularity was increased by at least 0.001 compress the graph and repeat
      // halt immediately if the community labeling took less than 3 passes
      if (numberOfPasses > 2 && currentQModularityValue > q_modularityValue + 0.001) {
        q_modularityValue = currentQModularityValue
        louvainGraph = louvainCore.compressGraph(louvainGraph)
      }
      else {
        halt = true
      }

    } while (!halt)
    save(louvainGraph, outputPath + "/final_vertices", outputPath + "/final_edges")

    sc.stop()
  }

  def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainData, Long], outputPath: String) = {
    val vertexOutput = outputPath + "/level_" + level + "_vertices"
    val edgeOutput = outputPath + "/level_" + level + "_edges"
    save(graph, vertexOutput, edgeOutput)
    qValues = qValues :+ ((level, q))
    sc.parallelize(qValues, 1).saveAsTextFile(outputPath + "/qvalues" + level)
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String, edegePath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
    graph.vertices.saveAsTextFile(edegePath)
  }

}
