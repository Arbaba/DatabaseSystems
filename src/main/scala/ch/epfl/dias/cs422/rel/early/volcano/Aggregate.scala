package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  var data :IndexedSeq[Tuple] = IndexedSeq()
  var nResults = 1
  var count = 0
  var processed :IndexedSeq[List[Any]]= IndexedSeq()
  override def open(): Unit = {
    input.open()
    data = input.iterator.toIndexedSeq
    val groupsIndexes = for( i <- (0 until groupSet.length()) if groupSet.get(i))yield i
    //Concatenate toString() of relevant indexes
    val groupedBy =data.groupBy(tuple => tuple.indices.map(i => if(groupsIndexes.contains(i)) tuple(i).toString else "").reduce(_ ++ _))
    if(groupSet.length() == 0){
      processed = IndexedSeq(for(call <- aggCalls) yield {data.foldLeft(call.emptyValue)((acc, tuple) => call.reduce(acc, call.getArgument(tuple)))})
    }else {
      processed = groupedBy.values.map{tuples => for(call <- aggCalls) yield {tuples.foldLeft(call.emptyValue)((acc, tuple) => call.reduce(acc, call.getArgument(tuple)))}}.toIndexedSeq

    }

  }

  override def next(): Tuple = {
    if(count >= processed.length){
      null
    }else {
      val tmp = processed(count)
      count += 1
      tmp.toIndexedSeq
    }
  }

  override def close(): Unit = input.close()
}
