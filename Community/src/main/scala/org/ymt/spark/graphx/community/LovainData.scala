package org.ymt.spark.graphx.community

/**
  * Created by yangmutong on 2017/4/18.
  */

class LovainData(var community: Long, var communitySigmaTot: Long, var internalWeight: Long, var nodeWeight: Long, var changed: Boolean) extends Serializable{
  def this() = this(-1L, 0L, 0L, 0L, false)
  override def toString: String = s"{community:$community,communitySigmaTot:$communitySigmaTot,internalWeight:$internalWeight,nodeWeight:$nodeWeight}"
}
