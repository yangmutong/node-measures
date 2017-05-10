package org.ymt.spark.graphx.community
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by yangmutong on 2017/4/14.
  */
object Louvain extends Serializable{

  var qValues = Array[(Int, Double)]()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Louvain")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(Louvain.getClass, classOf[LouvainCore], classOf[LouvainData]))
    val sc = new SparkContext(conf)
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt
    @transient lazy val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    // graph loader phase
    val g = makeGraph(inputPath, sc, numPartitions).persist()
    // computation phase
    val louvainCore = new LouvainCore
    var louvainGraph = louvainCore.createLouvainGraph(g.mapEdges(v => v.attr.toLong))

    var compressionLevel = -1 // number of times the graph has been compressed
    var q_modularityValue = -1.0 // current modularity value
    var halt = false
    do {
      compressionLevel += 1
      log.warn(s"\nStarting Louvain level $compressionLevel")

      // label each vertex with its best community choice at this level of compression
      val (currentQModularityValue, currentGraph, numberOfPasses) = louvainCore.louvain(sc, louvainGraph)
      louvainGraph.unpersistVertices(blocking = false)
      louvainGraph = currentGraph

      // not now
      // saveLevel(sc, compressionLevel, currentQModularityValue, louvainGraph, outputPath)

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
    save(louvainGraph, outputPath + "/final_vertices")

    sc.stop()
  }

  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext, numPartitions: Int): Graph[Double, Double] = {
    GraphLoader.edgeListFile(sc, inputPath, numEdgePartitions=numPartitions).unpersist()
      .partitionBy(PartitionStrategy.EdgePartition2D).unpersist()
      .mapVertices((vid, attr) => attr.toDouble).unpersist()
      .mapEdges(v => v.attr.toDouble)
  }

  def saveLevel(sc: SparkContext, level: Int, q: Double, graph: Graph[LouvainData, Long], outputPath: String) = {
    val vertexOutput = outputPath + "/level_" + level + "_vertices"
    save(graph, vertexOutput)
    qValues = qValues :+ ((level, q))
    sc.parallelize(qValues, 1).saveAsTextFile(outputPath + "/qvalues" + level)
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
  }

}
