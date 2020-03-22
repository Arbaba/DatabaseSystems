package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {-
  override def execute(): IndexedSeq[Column] = {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))
    ???
  }

  private lazy val evals = ???

  override def evaluators(): LazyEvaluatorAccess = evals
}
