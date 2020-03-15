package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  override def open(): Unit = {
    left.open()
    right.open()
  }

  override def next(): Tuple = (left.next(), right.next()) match {

    case (l: Tuple,r: Tuple)   => l
    case _ => null
  }

  override def close(): Unit = ???
}
