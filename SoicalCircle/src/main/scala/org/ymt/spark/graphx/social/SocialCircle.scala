package org.ymt.spark.graphx.social

/**
  * Created by yangmutong on 2017/3/27.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object SocialCircle extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Intro"))
    val baseUrl = "hdfs://master:8020/user/ymt/"
    val egonets = sc.wholeTextFiles(baseUrl + "egonets")
    val egonetsNum = egonets.map(x => extractId(x._1)).collect()
    val egonetsEdges = egonets.map(x => makeEdges(x._2)).collect()
    val circle = egonetsEdges.toList.map(x => getCircles(sc, x))
    val result = egonetsNum.zip(circle).map(x => x._1 + "," + x._2)

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
