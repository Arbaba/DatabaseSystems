package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  override def open(): Unit = input.open()

  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  override def next(): Tuple =  {
    val v = input.next()
    v match {
      case t: Tuple => e(t) match {
        case true => t
        case _ => next()
      }
      case _ => next()
    }
  }

  override def close(): Unit = input.close()
}
