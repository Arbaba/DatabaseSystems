package ch.epfl.dias.cs422.helpers.builder.skeleton

import ch.epfl.dias.cs422.helpers.Construct
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.{RelCollation, RelNode, core}
import org.apache.calcite.rex.RexNode

abstract class Sort[TOperator <: RelNode] protected (input: TOperator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends core.Sort(input.getCluster, input.getTraitSet.replace(collation), input, collation, offset, fetch) {
  self: TOperator =>

  override def copy(traitSet: RelTraitSet, newInput: RelNode, newCollation: RelCollation, offset: RexNode, fetch: RexNode): Sort[TOperator] = {
    Sort.create(newInput.asInstanceOf[TOperator], newCollation, offset, fetch, this.getClass)
  }
}

object Sort {
  def create[TOperator <: RelNode](child: TOperator, collation: RelCollation, offset: RexNode, fetch: RexNode, c: Class[_ <: Sort[TOperator]]): Sort[TOperator] = {
    Construct.create(c, child, collation, offset, fetch)
  }
}
