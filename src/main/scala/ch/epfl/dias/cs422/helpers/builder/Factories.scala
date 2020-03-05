package ch.epfl.dias.cs422.helpers.builder

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton._
import ch.epfl.dias.cs422.helpers.rex
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.{RelOptCluster, RelOptTable}
import org.apache.calcite.rel.core.{AggregateCall, CorrelationId, JoinRelType, RelFactories}
import org.apache.calcite.rel.{RelCollation, RelNode}
import org.apache.calcite.rex.{RexNode, RexUtil}
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.ImmutableBitSet

import scala.reflect._
import scala.jdk.CollectionConverters._

object Factories {
  val VOLCANO_INSTANCE = new Factories[
    ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    ch.epfl.dias.cs422.rel.early.volcano.Filter,
    ch.epfl.dias.cs422.rel.early.volcano.Scan,
    ch.epfl.dias.cs422.rel.early.volcano.Project,
    ch.epfl.dias.cs422.rel.early.volcano.Aggregate,
    ch.epfl.dias.cs422.rel.early.volcano.Sort,
    ch.epfl.dias.cs422.rel.early.volcano.Join
  ]()
  val BLOCKATATIME_INSTANCE = new Factories[
    ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator,
    ch.epfl.dias.cs422.rel.early.blockatatime.Filter,
    ch.epfl.dias.cs422.rel.early.blockatatime.Scan,
    ch.epfl.dias.cs422.rel.early.blockatatime.Project,
    ch.epfl.dias.cs422.rel.early.blockatatime.Aggregate,
    ch.epfl.dias.cs422.rel.early.blockatatime.Sort,
    ch.epfl.dias.cs422.rel.early.blockatatime.Join
  ]()
  val OPERATOR_AT_A_TIME_INSTANCE = new Factories[
    ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    ch.epfl.dias.cs422.rel.early.operatoratatime.Filter,
    ch.epfl.dias.cs422.rel.early.operatoratatime.Scan,
    ch.epfl.dias.cs422.rel.early.operatoratatime.Project,
    ch.epfl.dias.cs422.rel.early.operatoratatime.Aggregate,
    ch.epfl.dias.cs422.rel.early.operatoratatime.Sort,
    ch.epfl.dias.cs422.rel.early.operatoratatime.Join
  ]()
  val LAZY_OPERATOR_AT_A_TIME_INSTANCE = new Factories[
    ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator,
    ch.epfl.dias.cs422.rel.late.operatoratatime.Filter,
    ch.epfl.dias.cs422.rel.late.operatoratatime.Scan,
    ch.epfl.dias.cs422.rel.late.operatoratatime.Project,
    ch.epfl.dias.cs422.rel.late.operatoratatime.Aggregate,
    ch.epfl.dias.cs422.rel.late.operatoratatime.Sort,
    ch.epfl.dias.cs422.rel.late.operatoratatime.Join
  ]()
}

class Factories[TOperator <: RelNode,
  TFilter <: TOperator with Filter[TOperator] : ClassTag,
  TScan <: TOperator with Scan[TOperator] : ClassTag,
  TProject <: TOperator with Project[TOperator] : ClassTag,
  TAggregate <: TOperator with Aggregate[TOperator] : ClassTag,
  TSort <: TOperator with Sort[TOperator] : ClassTag,
  TJoin <: TOperator with Join[TOperator] : ClassTag] private() {

  type OpType = TOperator

  val FILTER_FACTORY: RelFactories.FilterFactory = (input: RelNode, condition: RexNode, _: util.Set[CorrelationId]) => Filter.create(input.asInstanceOf[TOperator], condition, classTag[TFilter].runtimeClass.asInstanceOf[Class[TFilter]])

  class ScanFactory extends RelFactories.TableScanFactory{
    override def createScan(cluster: RelOptCluster, table: RelOptTable): RelNode = Scan.create(cluster, table, classTag[TScan].runtimeClass.asInstanceOf[Class[TScan]], (s: ScannableTable) => s.getAsRowStore)

    def createScan(cluster: RelOptCluster, table: RelOptTable, tableToScan: ScannableTable => Store): RelNode = Scan.create(cluster, table, classTag[TScan].runtimeClass.asInstanceOf[Class[TScan]], tableToScan)
  }

  val SCAN_FACTORY: ScanFactory = new ScanFactory

  val PROJECT_FACTORY: RelFactories.ProjectFactory = (input: RelNode, childExprs: util.List[_ <: RexNode], fieldNames: util.List[String]) => {
    val rowType = RexUtil.createStructType(input.getCluster.getTypeFactory, childExprs, fieldNames, SqlValidatorUtil.F_SUGGESTER)
    Project.create(input.asInstanceOf[TOperator], childExprs, rowType, classTag[TProject].runtimeClass.asInstanceOf[Class[TProject]])
  }

  val JOIN_FACTORY: RelFactories.JoinFactory = (left: RelNode, right: RelNode, condition: RexNode, variablesSet: util.Set[CorrelationId], joinType: JoinRelType, _: Boolean) => {
    assert(joinType == JoinRelType.INNER)
    assert(variablesSet.size() == 0)
    Join.create(left.asInstanceOf[TOperator], right.asInstanceOf[TOperator], condition, classTag[TJoin].runtimeClass.asInstanceOf[Class[TJoin]])
  }
  val SORT_FACTORY: RelFactories.SortFactory = (input: RelNode, collation: RelCollation, offset: RexNode, fetch: RexNode) => {
    Sort.create(input.asInstanceOf[TOperator], collation, offset, fetch, classTag[TSort].runtimeClass.asInstanceOf[Class[TSort]])
  }
  val AGGREGATE_FACTORY: RelFactories.AggregateFactory = (input: RelNode, groupSet: ImmutableBitSet, groupSets: ImmutableList[ImmutableBitSet], aggCalls: util.List[AggregateCall]) => {
    assert(groupSets == null || (groupSets.size() == 1 && groupSets.get(0) == groupSet))
    Aggregate.create(input.asInstanceOf[TOperator], groupSet, aggCalls.asScala.toList.map(a => rex.AggregateCall(a)), classTag[TAggregate].runtimeClass.asInstanceOf[Class[TAggregate]])
  }
}
