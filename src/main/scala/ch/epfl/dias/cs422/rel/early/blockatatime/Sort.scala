package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Block
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.rel.early.volcano.Wrapper
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.SortedSet
class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  var ndiscarded :Int = offset match {
    case null  => 0
    case _ => evalLiteral(offset).asInstanceOf[Int]
  }
  var buffer = new Buffer(blockSize)

  var nfetch :Int = fetch match {
    case null  => Int.MaxValue
    case _ => evalLiteral(fetch).asInstanceOf[Int]
  }
  var data : SortedSet[Wrapper] = SortedSet()
  var sortedData : Block =IndexedSeq()
  override def open(): Unit = {
    for(tuple <-input.iterator.toList.flatten.drop(ndiscarded).take(nfetch)){
      data+= Wrapper(tuple, collation)
    }
    sortedData  = data.iterator.toIndexedSeq.map(x => x.getTuple)


  }

  override def next(): Block = {
    val tmp = sortedData.take(blockSize)
    sortedData = sortedData.drop(blockSize)

    if(tmp.isEmpty) null
    else tmp
  }

  override def close(): Unit = input.close()
}
