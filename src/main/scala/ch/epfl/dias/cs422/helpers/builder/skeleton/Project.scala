package ch.epfl.dias.cs422.helpers.builder.skeleton

import java.util

import ch.epfl.dias.cs422.helpers.Construct
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, core}
import org.apache.calcite.rex.RexNode

abstract class Project[TOperator <: RelNode] protected (input: TOperator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends core.Project(input.getCluster, input.getTraitSet, input, projects, rowType) {
  self: TOperator =>
  final override def copy(traitSet: RelTraitSet, input: RelNode, projects: util.List[RexNode], rowType: RelDataType): Project[TOperator] = {
    Project.create(input.asInstanceOf[TOperator], projects, rowType, this.getClass)
  }
}

object Project {
  def create[TOperator <: RelNode](input: TOperator, projects: java.util.List[_ <: RexNode], rowType: RelDataType, c: Class[_ <: Project[TOperator]]): Project[TOperator] = {
    Construct.create(c, input, projects, rowType)
  }
}

