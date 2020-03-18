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

    implicit def anyflattener[A](a: A) : Iterable[A] = Some(a)

    def asRows(d: IndexedSeq[Column]) = for (row <- (0 until d(0).length)) yield ((0 until d.length).map(col => d(col)(row))).flatten

    def asCols(d: IndexedSeq[Tuple]) = for (col <- (0 until d(0).length)) yield ((0 until d.length).map(row => d(row)(col))).flatten

    val data = input.execute()
    if(data.length == 0){
      IndexedSeq()
    }else {
      val rowed = asRows(data)
      println("rowed " + rowed)
      asCols(rowed.map( x => evaluator(x)))

    }
  }
}
