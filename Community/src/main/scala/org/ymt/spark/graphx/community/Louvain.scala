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


    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val graph = Graph(myVertices, myEdges)

    val outputPath: String = args(0)

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
