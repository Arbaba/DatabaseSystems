package ch.epfl.dias.cs422.helpers.rel

import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rex.{RexExecutor, TupleContext}
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

protected[helpers] object RelOperatorUtilLog {
  var accesses: Long = 0

  // FIXME: restrinct access
  def resetFieldAccessCount(): Unit = {
    accesses = 0
  }

  def getFieldAccessCount: Long = accesses
}

object RelOperatorUtil{
  protected [helpers] def eval(cluster: RelOptCluster, e: RexNode, inputRowType: RelDataType): Tuple => Elem = {
    val f = eval(cluster, IndexedSeq(e), inputRowType)
    tuple => f(tuple)(0)
  }

  protected [helpers] def eval(cluster: RelOptCluster, e: IndexedSeq[RexNode], inputRowType: RelDataType): Tuple => Tuple = {
    val f = cluster.getPlanner.getExecutor.asInstanceOf[RexExecutor].getExecutable(
      cluster.getRexBuilder,
      e.asJava,
      inputRowType
    )
    val fun = f.getFunction

    def t(tuple: Tuple): Tuple = {
      fun.apply(new TupleContext {
        override def field(index: Int): Any = {
          assert(tuple != null, "You asked to evaluate an expression on a Nil tuple")
          if (tuple.size != inputRowType.getFieldCount) println(tuple + " " + tuple.size + " " + inputRowType + " " + inputRowType.getFieldCount)
          assert(tuple.size == inputRowType.getFieldCount, "You provided a tuple that does not match the specifidied inputRowType")
//          println("Accessing field: " + index)
          tuple(index)
        }
      }).toIndexedSeq.asInstanceOf[Tuple]
    }

    t
  }
}