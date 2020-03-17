package ch.epfl.dias.cs422.rel.early.operatoratatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode
import scala.jdk.CollectionConverters._

class Project protected(input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  lazy val evaluator: Tuple => Tuple = eval(projects.asScala.toIndexedSeq, input.getRowType)

  override def execute(): IndexedSeq[Column] = {


    def asRows(d: IndexedSeq[Column]) = for(row <- (0 until d.length)) yield (0 until d.length ).map(col => d(col)(row))
    def asCols(d: IndexedSeq[Tuple]) = for(col <- (0 until d(0).length)) yield (0 until d.length ).map(row => d(row)(col))
    val data = input.execute()
    if(data.length == 0){
      IndexedSeq()
    }else {
      asCols(asRows(data).filter( x => evaluator(x).asInstanceOf[Boolean]))

    }
  }
}
