package ch.epfl.dias.cs422.helpers.qo.rules

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptUtil}
import org.apache.calcite.rel.core.{Join, RelFactories}
import org.apache.calcite.rex.{RexLiteral, RexUtil}
import org.apache.calcite.tools.RelBuilderFactory

object JoinPushPredicates {
  val INSTANCE = new JoinPushPredicates(RelFactories.LOGICAL_BUILDER)
}

class JoinPushPredicates(relBuilderFactory: RelBuilderFactory)
    extends RelOptRule(RelOptRule.operand(classOf[Join], RelOptRule.any), relBuilderFactory, "Decompose complex join") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val l1 = new java.util.ArrayList[Integer]
    val l2 = new java.util.ArrayList[Integer]
    val l3 = new java.util.ArrayList[java.lang.Boolean]

    val expr = RexUtil.pullFactors(join.getCluster.getRexBuilder,  join.getCondition)
    val l0 = RelOptUtil.splitJoinCondition(join.getLeft, join.getRight, expr, l1, l2, l3)
    if (l3.size() != 1 || !l3.get(0)) return
    if (l1.isEmpty && l2.isEmpty) return
    l0 match {
      case literal: RexLiteral if literal.isAlwaysTrue => return
      case _ =>
    }

    val cond = RelOptUtil.createEquiJoinCondition(join.getLeft, l1, join.getRight, l2, join.getCluster.getRexBuilder)

    call.transformTo(
      call.builder
        .push(join.copy(join.getTraitSet, cond, join.getLeft, join.getRight, join.getJoinType, join.isSemiJoinDone))
        .filter(l0)
        .build()
    )
  }
}
