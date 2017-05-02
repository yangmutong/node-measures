package org.ymt.spark.graphx.social

/**
  * Created by yangmutong on 2017/3/27.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object SocialCircle extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Intro"))
    val baseUrl = "hdfs://master:8020/user/ymt/"
    val egonets = sc.wholeTextFiles(baseUrl + "egonets")
    val egonetsNum = egonets.map(x => extractId(x._1)).collect()
    val egonetsEdges = egonets.map(x => makeEdges(x._2)).collect()
    val circle = egonetsEdges.toList.map(x => getCircles(sc, x))
    val result = egonetsNum.zip(circle).map(x => x._1 + "," + x._2)

    val myVertices = sc.makeRDD(Array((1L, "A"), (2L, "B"), (3L, "C"),
      (4L, "D"), (5L, "E"), (6L, "F"), (7L, "G")))
    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, 7.0), Edge(1L, 4L, 5.0),
      Edge(2L, 3L, 8.0), Edge(2L, 4L, 9.0), Edge(2L, 5L, 7.0),
      Edge(3L, 5L, 5.0), Edge(4L, 5L, 15.0), Edge(4L, 6L, 6.0),
      Edge(5L, 6L, 8.0), Edge(5L, 7L, 9.0), Edge(6L, 7L, 11.0)))
    val myGraph = Graph(myVertices, myEdges)
    val partitionNum = 8
    val edgesRepartitionRdd = myEdges.map(
      edge => {
        val pid = PartitionStrategy.EdgePartition2D.getPartition(edge.srcId, edge.dstId, partitionNum)
        (pid, (edge.srcId, edge.dstId))
      }

    ).partitionBy(new HashPartitioner(partitionNum)).map {
      case (pid, (src: Long, dst: Long)) =>
        Edge(src, dst, 1)
    }
    Graph.fromEdges(edgesRepartitionRdd, 0L)

    sc.stop()
  }

  def extractId(s: String) = {
    val pattern = """^.*?(\d+).egonet""".r
    val pattern(num) = s
    num
  }

  def getEdges(line: String): Array[(Long, Long)] = {
    val arr = line.split(":")
    val srcId = arr(0).toLong
    val dstIds = arr(1).split(" ")
    val edges = dstIds.filter(d => !d.equals("")).map(dstId => (srcId, dstId.toLong))
    if (edges.size > 0) edges else Array((srcId, srcId))
  }

  def makeEdges(contents: String) = {
    val lines = contents.split("\n")
    val unflat = lines.map(line => getEdges(line))
    val flat = unflat.flatten
    flat
  }

  def getCircles(sc: SparkContext, flat: Array[(Long, Long)]) = {
    val edges = sc.parallelize(flat)
    val g = Graph.fromEdgeTuples(edges, 1)
    val cc = g.connectedComponents()
    cc.vertices.map(x => (x._2, Array(x._1))).reduceByKey((a, b) => a ++ b)
      .values.map(_.mkString(" ")).collect.mkString(";")
  }

}
