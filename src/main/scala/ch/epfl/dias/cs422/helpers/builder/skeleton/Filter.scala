package ch.epfl.dias.cs422.helpers.builder.skeleton

import ch.epfl.dias.cs422.helpers.Construct
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.{core, _}
import org.apache.calcite.rex.RexNode

abstract class Filter[TOperator <: RelNode] protected (input: TOperator, condition: RexNode) extends core.Filter(input.getCluster, input.getTraitSet, input, condition) {
  self: TOperator =>
  final override def copy(traitSet: RelTraitSet, input: RelNode, condition: RexNode): Filter[TOperator] = Filter.create(input.asInstanceOf[TOperator], condition, this.getClass)
}

object Filter{
  def create[TOperator <: RelNode](input: TOperator, condition: RexNode, c: Class[_ <: Filter[TOperator]]): Filter[TOperator] = {
    Construct.create(c, input, condition)
  }
}