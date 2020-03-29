package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  lazy val e: Tuple => Any = eval(condition, input.getRowType)
  var buffer: Buffer = new Buffer(blockSize)

  override def open(): Unit = {
    buffer = new Buffer(blockSize)
    input.open()
  }
/*
  def fillBuffer(): Unit = {
    if(buffer.length <= blockSize){
      val v = input.next()
      v match {
        case null =>

        case t =>
          register(t.filter(x => e(x).asInstanceOf[Boolean]))
          fillBuffer()
      }
    }
  }*/
  override def next(): Block = {
    val data :Unit => Block = { _ =>
      input.next
    }
    val tuples = {
      buffer.fillBuffer(data, tuples => tuples.filter(x => e(x).asInstanceOf[Boolean]))
      val t = buffer.flush()
      t
    }

    if(tuples.length > 0){
      tuples
    }else{
      null
    }
    //input.next()
  }

  override def close(): Unit = input.close()
}
