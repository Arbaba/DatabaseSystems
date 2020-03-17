package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

class Project protected (input: Operator, projects: java.util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  override def open(): Unit = input.open()

  lazy val evaluator: Tuple => Tuple = eval(projects.asScala.toIndexedSeq, input.getRowType)

  override def next(): Tuple = input.next() match {
    case null =>
      println("projection over")
      null
    case x =>
      println("Project: " + x + " " + evaluator(x))
      evaluator(x)
  }

  override def close(): Unit = input.close()

}
