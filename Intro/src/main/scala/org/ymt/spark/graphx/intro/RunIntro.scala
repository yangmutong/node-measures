package org.ymt.spark.graphx.intro

/**
  * Created by yangmutong on 2017/3/26.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._


object RunIntro extends Serializable {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Intro"))
    val baseUrl = "hdfs://master:8020/user/ymt/"

    val graph = GraphLoader.edgeListFile(sc, baseUrl + "arxiv/cit-HepPh.txt")
    println("入度最大的顶点:" + graph.inDegrees.reduce((pre, cur) => if (pre._2 > cur._2) pre else cur))

    graph.vertices.first()

    println(graph.pageRank(0.009).vertices.take(10))
  }
}

