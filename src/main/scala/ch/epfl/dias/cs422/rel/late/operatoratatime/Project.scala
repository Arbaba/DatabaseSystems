package ch.epfl.dias.cs422.rel.late.operatoratatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluator
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode
import scala.jdk.CollectionConverters._

class Project protected(input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  override def execute(): IndexedSeq[Column] = {
    input.execute()
  }
  private lazy val evals = lazyEval(projects.asScala.toIndexedSeq, input.getRowType, input.evaluators())
  override def evaluators(): LazyEvaluator = evals
}
