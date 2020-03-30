package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {


  override def execute(): IndexedSeq[Column] = {
    val data = input.iterator.toIndexedSeq

    if (data.length == 0 && groupSet.cardinality() == 0) {
      (for (call <- aggCalls) yield {
        IndexedSeq(call.emptyValue)
      }).toIndexedSeq
    } else if (data.length == 0) {
      IndexedSeq()
    } else if (groupSet.cardinality() > 0) {
      val groupsIndexes = for (i <- (0 until groupSet.length()) if groupSet.get(i)) yield i
      val rowed = asRows(data)
      val groupedBy: Map[IndexedSeq[Elem], IndexedSeq[Tuple]] = rowed.groupBy(tuple => groupsIndexes.map { i => tuple(i) })

      val processed = groupedBy.map { case (k: IndexedSeq[Any], tuples: IndexedSeq[Tuple]) =>
        (k, k ++ (for (call <- aggCalls) yield {
          tuples.init.foldLeft(call.getArgument(tuples.last))((acc, tuple) => call.reduce(acc, call.getArgument(tuple)))
        }))
      }.values.toIndexedSeq

      asCols(processed)

    } else {

      aggrGroup(data)
    }
  }
  implicit def anyflattener[A](a: A) : Iterable[A] = Some(a)
  def getTuple(data: IndexedSeq[Column], i: Int): Tuple = data.flatMap { col: Column => col(i) }

  def aggrGroup(data: IndexedSeq[Column]): IndexedSeq[Column] = {
    val aggregates = for (call <- aggCalls) yield {
      val z = call.getArgument(getTuple(data, data(0).length - 1))
      (0 until data(0).length - 1).foldLeft(z)((acc, tupleidx) => call.reduce(acc, call.getArgument(getTuple(data, tupleidx))))
    }

    aggregates.toIndexedSeq.map(a => IndexedSeq(a))
  }

}
