package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
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


    val groupedBy: Map[IndexedSeq[Elem], IndexedSeq[Tuple]] =data.groupBy(tuple => groupsIndexes.map{ i => tuple(i)})

    if(groupSet.length() == 0){
      processed = IndexedSeq(for(call <- aggCalls) yield {
        if(data.length == 0) call.emptyValue
        else data.init.foldLeft(call.getArgument(data.last))((acc, tuple) => call.reduce(acc, call.getArgument(tuple)))
      })
    }else {

      processed = groupedBy.map{case (k :IndexedSeq[Any],tuples :IndexedSeq[Tuple]) =>
        (k, k ++ (for(call <- aggCalls) yield { tuples.init.foldLeft(call.getArgument(tuples.last))((acc, tuple) => call.reduce(acc, call.getArgument(tuple)))
                                              }))
      }.values.map{case v : IndexedSeq[Any] => v.toList}.toIndexedSeq

    }
    println("aggre")
  println(processed)
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
