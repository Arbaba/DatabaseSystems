package ch.epfl.dias.cs422.helpers.rex

import org.apache.calcite.rel.core
import org.apache.calcite.runtime.SqlFunctions
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.fun.{SqlAvgAggFunction, SqlCountAggFunction, SqlMinMaxAggFunction, SqlSumAggFunction, SqlSumEmptyIsZeroAggFunction}

import scala.jdk.CollectionConverters._

final case class AggregateCall(private[helpers] val agg: core.AggregateCall) {

  def reduce(e1: Any, e2: Any): Any = {
    agg.getAggregation match {
      case _: SqlSumEmptyIsZeroAggFunction => SqlFunctions.plusAny(e1, e2)
      case _: SqlSumAggFunction => SqlFunctions.plusAny(e1, e2)
      case _: SqlCountAggFunction => SqlFunctions.plusAny(e1, e2)
      case a: SqlMinMaxAggFunction =>
        val comp = e1 match {
          case b1: Boolean => b1 || !e2.asInstanceOf[Boolean]
          case _ => SqlFunctions.geAny(e1, e2)
        }
        if ((a.getKind == SqlKind.MAX) == comp) e1
        else e2
      case _: SqlAvgAggFunction => assert(false) // Should never happen
    }
  }

  def getArgument[Tuple <: IndexedSeq[Any]](e: Tuple): Any = {
    agg.getArgList.asScala.toList match {
      case x :: Nil => e(x)
      case Nil => 1
      case _ => new IllegalArgumentException("Unexpected arg list with size >= 2")
    }
  }

  def emptyValue: Any = {
    agg.getAggregation match {
      case _: SqlSumEmptyIsZeroAggFunction => 0
      case _: SqlSumAggFunction => null
      case _: SqlCountAggFunction => 0
      case _: SqlMinMaxAggFunction => null
    }
  }

}
