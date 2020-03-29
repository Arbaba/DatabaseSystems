package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  override def execute(): IndexedSeq[Column] = {
    if(groupSet.cardinality() == 0 ){
      IndexedSeq(IndexedSeq(0.toLong))
    }else if(input.execute().length == 0) {
      IndexedSeq()
    }else{
      val groupsIndexes = for (i <- (0 until groupSet.length()) if groupSet.get(i)) yield i
      val groupedBy: Map[IndexedSeq[Elem], IndexedSeq[Tuple]] = input.execute().map(x => input.evaluators()(x)).groupBy(tuple => groupsIndexes.map { i => tuple(i) })
      val vids = for (i <- (0 until groupedBy.size) ) yield IndexedSeq(i.toLong)
      vids
    }
  }

  private lazy val evals = {
    val vids = input.execute()
    if (vids.length == 0 && groupSet.cardinality() == 0) {
      val fs = for (call <- aggCalls) yield {
        _ :Long => call.emptyValue
      }
      new LazyEvaluatorAccess(fs)
    }else if(groupSet.cardinality() > 0 ) {
      val groupsIndexes = for (i <- (0 until groupSet.length()) if groupSet.get(i)) yield i
      val groupedBy: Map[IndexedSeq[Elem], IndexedSeq[Tuple]] = input.execute().map(x => input.evaluators()(x)).groupBy(tuple => groupsIndexes.map { i => tuple(i) })
      val groups : IndexedSeq[IndexedSeq[Tuple]] = groupedBy.values.toIndexedSeq

      val fs = for (call <- aggCalls) yield {
        row :Long => groups(row.toInt).init.foldLeft(call.getArgument(groups(row.toInt).last))((acc, tuple) => call.reduce(acc, call.getArgument(tuple)))
      }
      val normalCols = for(col <-  0 until groupsIndexes.length ) yield {
        row :Long => groupedBy.keys.toList(row.toInt)(col).asInstanceOf[Any]
      }
      new LazyEvaluatorAccess((normalCols ++ fs).toList)
    }else {
      val data :IndexedSeq[Tuple] =  input.execute().map(x => input.evaluators()(x))
      val fs = for (call <- aggCalls) yield {
        row :Long =>{
          println(row)

          data.init.foldLeft(call.getArgument(data.last))((acc, tuple) => call.reduce(acc, call.getArgument(tuple)))
        }
      }
      new LazyEvaluatorAccess(fs)
    }
  }

  override def evaluators(): LazyEvaluatorAccess = evals
}
