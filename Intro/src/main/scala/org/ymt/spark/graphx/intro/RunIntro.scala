package org.ymt.spark.graphx.intro

/**
  * Created by yangmutong on 2017/3/26.
  */
import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.graphx.util._

object RunIntro extends Serializable {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Intro"))
    val baseUrl = "hdfs://master:8020/user/ymt/"
    val edges = GraphLoader.edgeListFile(sc, baseUrl + "arxiv/cit-HepPh.txt").edges
    val vertices = sc.textFile(baseUrl + "arxiv/cit-HepPh-dates.txt")
      .filter(f => !f.contains("#"))
      .map(s => {
        val arr = s.split(" ")
        (arr(0).toLong, arr(1))
      })
    val graph = Graph(vertices, edges)

    graph.aggregateMessages[Int](_.sendToSrc(1), _ + _)
    // mllib.recommendation.ALS
    // graphx.lib.SVDPlusPlus

    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)

    propagateEdgeCount(myGraph.mapVertices((_, _) => 0)).vertices.collect()

    val pw = new PrintWriter("myGraph.gexf")
    pw.write(toGexf(myGraph))
    pw.close()
    sc.stop()
  }

  def sendMsg(ec: EdgeContext[Int, String, Int]): Unit = {
    ec.sendToDst(ec.srcAttr + 1)
  }

  def mergeMsg(pre: Int, curr: Int): Int = {
    math.max(pre, curr)
  }

  def propagateEdgeCount(g: Graph[Int, String]): Graph[Int, String] = {
    val g2 = Graph(g.aggregateMessages[Int](sendMsg, mergeMsg), g.edges)
    val check = g2.vertices.join(g.vertices).map(x => x._2._1 - x._2._2).reduce(_ + _)
    if (check > 0)
      propagateEdgeCount(g2)
    else
      g
  }

  def toGexf[VD,ED](g:Graph[VD,ED]) =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
      "  <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
      "    <nodes>\n" +
      g.vertices.map(v => "      <node id=\"" + v._1 + "\" label=\"" +
        v._2 + "\" />\n").collect.mkString +
      "    </nodes>\n" +
      "    <edges>\n" +
      g.edges.map(e => "      <edge source=\"" + e.srcId +
        "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
        "\" />\n").collect.mkString +
      "    </edges>\n" +
      "  </graph>\n" +
      "</gexf>"

}

