package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {

    lazy val e: Tuple => Any = eval(condition, input.getRowType)
    val ncols = input.getRowType.getFieldList().size()
    val data = input.execute()
    if(data.length == 0){ IndexedSeq()
    }else{
      val nrows = data(0).length
      val rowsToKeep = for(row <- 0 until nrows;   if(e(getTuple(data, row)).asInstanceOf[Boolean]))   yield row
      val t = for(row <- rowsToKeep) yield getTuple(data, row)
      if(t.length > 0){
        println(t)
        asCols(t)
      }else {
        IndexedSeq()
      }

    }


  }
  implicit def anyflattener[A](a: A) : Iterable[A] = Some(a)
  def asCols(d: IndexedSeq[Tuple]) = for (col <- (0 until d(0).length)) yield ((0 until d.length).map(row => d(row)(col))).flatten

  def getTuple(data: IndexedSeq[Column], i: Int): Tuple = data.map{col : Column => col(i)}.flatten

}
