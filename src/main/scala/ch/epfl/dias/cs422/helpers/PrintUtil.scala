package ch.epfl.dias.cs422.helpers

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlExplainLevel

object PrintUtil {
  def printTree(rel: RelNode): Unit = {
    print(RelOptUtil.toString(rel, SqlExplainLevel.ALL_ATTRIBUTES))
  }

  def time[T](f: () => T, message: String = null): T = {
    val startTimeMillis = System.currentTimeMillis()
    val ret = f()
    val endTimeMillis = System.currentTimeMillis()
    println((if (message != null) message + ": " else "") + (endTimeMillis - startTimeMillis) + "ms")
    ret
  }
}
