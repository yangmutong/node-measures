package org.ymt.spark.graphx.cluster

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

/**
  * Created by yangmutong on 2017/4/10.
  */
object ClusterCoef extends Serializable{
  def main(args: Array[String]): Unit = {val conf = new SparkConf()
    conf.setAppName("Cluster Coefficient")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(ClusterCoef.getClass))
    val sc = new SparkContext(conf)
    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = args(2).toInt

    // graph loader phase
    val graph = makeGraph(inputPath, sc, numPartitions).persist()
    val result = clusterCoef(graph)
    save(result, outputPath + "/vertices")

    sc.stop()
  }

  def makeGraph[VD: ClassTag](inputPath: String, sc: SparkContext, numPartitions: Int): Graph[Double, Double] = {
    GraphLoader.edgeListFile(sc, inputPath, true, numEdgePartitions=numPartitions).unpersist()
      .partitionBy(PartitionStrategy.EdgePartition2D).unpersist()
      .mapVertices((vid, attr) => attr.toDouble).unpersist()
      .mapEdges(v => v.attr.toDouble)
  }
  def save[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], vertexPath: String): Unit = {
    graph.vertices.saveAsTextFile(vertexPath)
  }
  def clusterCoef[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    val triangleGraph = graph.triangleCount()
    val maxTriangleGraph = graph.degrees.mapValues(d => d * (d - 1) / 2.0)
    val result = triangleGraph.vertices.innerJoin(maxTriangleGraph) {(vid, triangleCount, maxTriangle) => {
      if (maxTriangle == 0) 0 else triangleCount / maxTriangle
    }}
    Graph(result, graph.edges)
  }
}
