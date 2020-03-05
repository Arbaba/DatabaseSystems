package ch.epfl.dias.cs422.helpers.builder.skeleton

import java.util

import ch.epfl.dias.cs422.helpers.Construct
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.core.{CorrelationId, JoinRelType}
import org.apache.calcite.rel.{RelNode, core}
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
 * The output of the Join operator should be the fields of the left child followed by the fields of the right child
 * The join is always an equijoin with the condition comparing the fields of the left child at indices getLeftKeys
 * with the fields of the right child at indices getRightKeys
 */
abstract class Join[TOperator <: RelNode](left: TOperator,
           right: TOperator,
           condition: RexNode) extends core.Join(left.getCluster, left.getTraitSet, left, right, condition, util.Set.of[CorrelationId](), JoinRelType.INNER) {
  self: TOperator =>

  private lazy val analyzeCond = analyzeCondition()

  protected final def getLeftKeys: IndexedSeq[Int] = {
    analyzeCond.leftKeys.asScala.toIndexedSeq.map(i => i.toInt)
  }

  protected final def getRightKeys: IndexedSeq[Int] = {
    analyzeCond.rightKeys.asScala.toIndexedSeq.map(i => i.toInt)
  }

  final override def copy(traitSet: RelTraitSet, condition: RexNode, left: RelNode, right: RelNode, joinType: JoinRelType,
                    semiJoinDone: Boolean): Join[TOperator] = {
    Join.create(left.asInstanceOf[TOperator], right.asInstanceOf[TOperator], condition, this.getClass)
  }
}

object Join{
  def create[TOperator <: RelNode](left: TOperator, right: TOperator, condition: RexNode, c: Class[_ <: Join[TOperator]]): Join[TOperator] = {
    Construct.create(c, left, right, condition)
  }
}