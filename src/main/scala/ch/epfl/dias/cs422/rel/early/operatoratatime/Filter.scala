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
      val rowsToKeep = for(row <- 0 until nrows;  rdata = (0 until ncols).map{col => data(row)(col)} if(e(rdata).asInstanceOf[Boolean]))   yield row
      for(row <- rowsToKeep) yield data(row)
    }

  }
}
