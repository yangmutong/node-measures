package org.ymt.spark.graphx.degree

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
  * Created by yangmutong on 2017/4/8.
  */
object DegreeCentrality extends Serializable{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Degree Centrality")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(DegreeCentrality.getClass))
    val sc = new SparkContext(conf)
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt

    // graph loader phase
    val graph = makeGraph(inputPath, sc, numPartitions).persist()
    val in = inDegreeCentrality(graph)
    val out = outDegreeCentrality(graph)
    val degree = degreeCentrality(graph)

    save(in, outputPath + "/in/vertices")
    save(out, outputPath + "/out/vertices")
    save(degree, outputPath + "/all/vertices")
    sc.stop()
  }



  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext, numPartitions: Int): Graph[Double, Double] = {
    GraphLoader.edgeListFile(sc, inputPath, true, numEdgePartitions=numPartitions).unpersist()
      .partitionBy(PartitionStrategy.EdgePartition2D).unpersist()
      .mapVertices((vid, attr) => attr.toDouble).unpersist()
      .mapEdges(v => v.attr.toDouble)
  }

  def save[VD: ClassTag](vertex: VertexRDD[VD], vertexPath: String): Unit = {
    vertex.saveAsTextFile(vertexPath)
  }
  def inDegreeCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Double] = {
    val length = graph.numVertices
    val s = 1.0 / (length - 1.0)
    graph.inDegrees.mapValues( (vid, value) => value * s)
  }

  def outDegreeCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Double] = {
    val length = graph.numVertices
    val s = 1.0 / (length - 1.0)
    graph.outDegrees.mapValues( (vid, value) => value * s)
  }

  def degreeCentrality[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): VertexRDD[Double] = {
    val length = graph.numVertices
    val s = 1.0 / (length - 1.0)
    graph.degrees.mapValues( (vid, value) => value * s)
  }
}
