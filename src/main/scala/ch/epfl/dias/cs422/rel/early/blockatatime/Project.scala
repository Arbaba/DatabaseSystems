package ch.epfl.dias.cs422.rel.early.blockatatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Block
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

class Project protected (input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  override def open(): Unit = ???

  override def next(): Block = ???

  override def close(): Unit = ???
}
