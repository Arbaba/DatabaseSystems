package ch.epfl.dias.cs422.helpers.builder.skeleton

import ch.epfl.dias.cs422.helpers.Construct
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan

abstract class Scan[TOperator <: RelNode] protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable) extends TableScan(cluster, traitSet, table) {
  self: TOperator =>
}


object Scan {
  def create[TOperator <: RelNode](cluster: RelOptCluster, table: RelOptTable, c: Class[_ <: Scan[TOperator]], tableToStore: ScannableTable => Store): Scan[TOperator] = Construct.create(c, cluster, cluster.traitSet(), table, tableToStore)
}