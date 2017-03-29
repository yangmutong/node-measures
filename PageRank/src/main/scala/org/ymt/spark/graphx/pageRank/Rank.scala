package org.ymt.spark.graphx.pageRank

/**
  * Created by yangmutong on 2017/3/26.
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._

object Rank extends Serializable {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Rank"))
    val baseUrl = "hdfs://master:8020/user/ymt/"

    val graph = GraphLoader.edgeListFile(sc, baseUrl + "arxiv/cit-HepPh.txt")

    graph.pageRank(0.0001)

    sc.stop()
  }
}


