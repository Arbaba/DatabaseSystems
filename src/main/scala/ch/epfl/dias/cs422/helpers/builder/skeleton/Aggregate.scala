package ch.epfl.dias.cs422.helpers.builder.skeleton

import java.util

import ch.epfl.dias.cs422.helpers.Construct
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.rel.core

import scala.jdk.CollectionConverters._

/**
 * The output of the aggregates contains the grouping keys as the first fields followed by the aggregates.
 * The grouping keys are at the indices pointed by groupSet.
 */
abstract class Aggregate[TOperator <: RelNode] protected (input: TOperator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends core.Aggregate(input.getCluster, input.getTraitSet, input, groupSet, List(groupSet).asJava, aggCalls.map(Aggregate.unwarp).asJava) {
  self: TOperator =>

  final override def copy(traitSet: RelTraitSet, input: RelNode, groupSet: ImmutableBitSet, groupSets: util.List[ImmutableBitSet], aggCalls: util.List[core.AggregateCall]): Aggregate[TOperator] = {
    Aggregate.create(input.asInstanceOf[TOperator], groupSet, aggCalls.asScala.map(a => AggregateCall(a)).toList, this.getClass)
  }
}

object Aggregate {
  def create[TOperator <: RelNode](input: TOperator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall], c: Class[_ <: Aggregate[TOperator]]): Aggregate[TOperator] = {
    Construct.create(c, input.asInstanceOf[TOperator], groupSet, aggCalls)
  }

  private def unwarp(aggregateCall: AggregateCall): core.AggregateCall = {
    aggregateCall.agg
  }
}
