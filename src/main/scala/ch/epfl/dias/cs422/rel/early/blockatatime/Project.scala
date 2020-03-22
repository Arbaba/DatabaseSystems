package ch.epfl.dias.cs422.rel.early.blockatatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode
import scala.jdk.CollectionConverters._

class Project protected (input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  var buffer: Buffer = new Buffer(blockSize)

  override def open(): Unit = {
    input.open()
     buffer = new Buffer(blockSize)

  }
  lazy val evaluator: Tuple => Tuple =  eval(projects.asScala.toIndexedSeq, input.getRowType)

  override def next(): Block =  {
      buffer.fillBuffer({_ => input.next()}, tuples => tuples.map(t => evaluator(t)))
      val data = buffer.flush()
    println("project " + data)
    if(data.isEmpty){
      null
    }else{
      data
    }
  }

  override def close(): Unit = input.close()
}
