package ch.epfl.dias.cs422.helpers

import java.util

import ch.epfl.dias.cs422.helpers.builder.Factories
import ch.epfl.dias.cs422.helpers.qo.rules.JoinPushPredicates
import ch.epfl.dias.cs422.helpers.rel.{RelOperator, RelOperatorUtilLog, early}
import ch.epfl.dias.cs422.helpers.rex.RexExecutor
import ch.epfl.dias.cs422.helpers.store.ScannableTable
import com.google.common.collect.ImmutableList
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.{HepMatchOrder, HepProgramBuilder}
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.rules._
import org.apache.calcite.rel.{RelNode, RelShuttle}
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.{SqlValidator, SqlValidatorUtil}
import org.apache.calcite.sql2rel.{RelDecorrelator, SqlToRelConverter}
import org.apache.calcite.tools.{Frameworks, Programs, RelBuilder}

import scala.jdk.CollectionConverters._

object SqlPrepare {
  private[helpers] var cluster: RelOptCluster = _
}

case class SqlPrepare[F <: Factories[_, _, _, _, _, _, _]](factory: F, storeType : String) {
  private def config: Frameworks.ConfigBuilder = {
    val rootSchema = Frameworks.createRootSchema(true)
    Schema.schema.foreach(e => rootSchema.add(e._1, new ScannableTable(e._2, e._1)))
    Frameworks
      .newConfigBuilder
      .parserConfig(SqlParser.Config.DEFAULT)
      .defaultSchema(rootSchema)
      .programs(
        Programs.heuristicJoinOrder(Programs.RULE_SET, false, 2)
      )
  }

  private val conf = config.context(Contexts.of(RelBuilder.Config.builder.build)).build
  private val builder = RelBuilder.create(conf)

  private val catalogReader = new CalciteCatalogReader(CalciteSchema.from(conf.getDefaultSchema), ImmutableList.of(""), builder.getTypeFactory, null)
  private val validator: SqlValidator = SqlValidatorUtil.newValidator(conf.getOperatorTable, catalogReader, builder.getTypeFactory, conf.getParserConfig.conformance())

  val cluster: RelOptCluster = {
    SqlPrepare.cluster = builder.getCluster
    builder.getCluster
  }
  private val converter = new SqlToRelConverter(conf.getViewExpander, validator, catalogReader, cluster, conf.getConvertletTable, conf.getSqlToRelConverterConfig)

  private val planner = {
    val planner = cluster.getPlanner

    planner match {
      case p: VolcanoPlanner => p.setNoneConventionHasInfiniteCost(false)
      case _ =>
    }

    val rules = planner.getRules
    rules.forEach(x => planner.removeRule(x))

    RelOptUtil.registerAbstractRules(planner)
    planner.addRule(ProjectMergeRule.INSTANCE)
    List(
//      FilterJoinRule.FILTER_ON_JOIN, // Disable FILTER_ON_JOIN to avoid merging complicated expressions into join
      FilterJoinRule.JOIN,
      ReduceExpressionsRule.FILTER_INSTANCE,
      ReduceExpressionsRule.JOIN_INSTANCE,
      ReduceExpressionsRule.PROJECT_INSTANCE,
      ValuesReduceRule.FILTER_INSTANCE,
      ValuesReduceRule.PROJECT_FILTER_INSTANCE,
      ValuesReduceRule.PROJECT_INSTANCE,
      AggregateValuesRule.INSTANCE
    ).foreach(planner.addRule)

//    planner.setExecutor(new RexExecutorImpl(Schemas.createDataContext(null, conf.getDefaultSchema)))
    planner.setExecutor(new RexExecutor())

//    List(
////      AggregateStarTableRule.INSTANCE,
////      AggregateStarTableRule.INSTANCE2,
//      TableScanRule.INSTANCE,
////      if (CalciteSystemProperty.COMMUTE.value) JoinAssociateRule.INSTANCE
////      else ProjectMergeRule.INSTANCE,
//      FilterTableScanRule.INSTANCE,
//      ProjectFilterTransposeRule.INSTANCE,
//      FilterProjectTransposeRule.INSTANCE,
//      FilterJoinRule.FILTER_ON_JOIN,
//      JoinPushExpressionsRule.INSTANCE,
//      AggregateExpandDistinctAggregatesRule.INSTANCE,
//      AggregateCaseToFilterRule.INSTANCE,
//      AggregateReduceFunctionsRule.INSTANCE,
//      FilterAggregateTransposeRule.INSTANCE,
//      ProjectWindowTransposeRule.INSTANCE,
//      MatchRule.INSTANCE,
////      JoinCommuteRule.INSTANCE,
////      JoinPushThroughJoinRule.RIGHT,
////      JoinPushThroughJoinRule.LEFT,
//      SortProjectTransposeRule.INSTANCE,
//      SortJoinTransposeRule.INSTANCE,
//      SortRemoveConstantKeysRule.INSTANCE,
//      SortUnionTransposeRule.INSTANCE,
//    ).foreach(planner.addRule)
    planner
  }

  private val avgToProjectsProgram = new HepProgramBuilder()
    .addRuleInstance(AggregateReduceFunctionsRule.INSTANCE)
    .addRuleInstance(ProjectMergeRule.INSTANCE)
    .addRuleInstance(ProjectRemoveRule.INSTANCE)
    .build()
  private val avgToProjects = Programs.of(avgToProjectsProgram, false, cluster.getMetadataProvider)

  def prepare(sql: String): RelNode = {
    val parser: SqlParser = SqlParser.create(sql, conf.getParserConfig)
    val sqlQuery = parser.parseQuery

    val log = converter.convertQuery(sqlQuery, true, true).rel

    // Replace LogicalCorrelate nodes with Joins
    val dec = RelDecorrelator.decorrelateQuery(log, builder)

    val rexSimplify = Programs.of(new HepProgramBuilder()
      .addRuleInstance(ReduceExpressionsRule.FILTER_INSTANCE)
      .addRuleInstance(ReduceExpressionsRule.JOIN_INSTANCE)
      .addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE)
      .addRuleInstance(ValuesReduceRule.FILTER_INSTANCE)
      .addRuleInstance(ValuesReduceRule.PROJECT_FILTER_INSTANCE)
      .addRuleInstance(ValuesReduceRule.PROJECT_INSTANCE)
      .addRuleInstance(AggregateValuesRule.INSTANCE)
      .build(), true, cluster.getMetadataProvider)

    // Gather together joins as a MultiJoin.
    val hep = new HepProgramBuilder()
      .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
      .addRuleInstance(FilterJoinRule.JOIN)
      .addMatchOrder(HepMatchOrder.BOTTOM_UP)
      .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
      .build()
    val toMultiJoin = Programs.of(hep, true, cluster.getMetadataProvider)

    // Program to expand a MultiJoin into heuristically ordered joins.
    // Do not add JoinCommuteRule and JoinPushThroughJoinRule, as
    // they cause exhaustive search.
    val joinOrdering = Programs.of(new HepProgramBuilder()
      .addRuleInstance(LoptOptimizeJoinRule.INSTANCE)
      .build(), true, cluster.getMetadataProvider)

    // Program to push complicated join conditions, below joins (eg. TPC-H Q19)
    val hepF = Programs.of(new HepProgramBuilder()
      .addRuleInstance(JoinPushPredicates.INSTANCE)
//      .addRuleInstance(FilterJoinRule.JOIN)
//      .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
//      .addRuleInstance(JoinExtractFilterRule.INSTANCE)
      .build(), true, cluster.getMetadataProvider)

//    val print: Program =
//      (_: RelOptPlanner, rel: RelNode, _: RelTraitSet, _: util.List[RelOptMaterialization], _: util.List[RelOptLattice]) => {
//        PrintUtil.printTree(rel)
//        rel
//      }

    val rj = Programs.sequence(
      // Simplify rex'es
      rexSimplify,
      // Join reordering
      toMultiJoin,
//      print,
      joinOrdering,
      // Move filters below joins
      hepF
    )
      .run(planner, dec, cluster.traitSet(), util.List.of[RelOptMaterialization](), util.List.of[RelOptLattice]())
//    PrintUtil.printTree(rj)

    // Optimize using a Volcano Optimizer
    planner.setRoot(rj)
    val r = planner.findBestExp()
//    PrintUtil.printTree(r)

    // Convert Aggregates with AVG, to Project(Aggregate(Sum, Cnt))
    val rel = avgToProjects.run(planner, r, cluster.traitSet(), util.List.of[RelOptMaterialization](), util.List.of[RelOptLattice]())

    // Force changing Logical operators to our operators
    val out = convert(rel)
    PrintUtil.printTree(out)
    out
  }


  def runQuery(sql: String): (List[IndexedSeq[Any]], RelDataType) = {
    RelOperatorUtilLog.resetFieldAccessCount()
    val operator =
      PrintUtil.time(() => {prepare(sql)}, "Prepare time")
    val tmp = PrintUtil.time(() => {
      (
        operator match {
          case op: ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator =>
            val evals = op.evaluators()
            val x = op.execute().map(n => evals(n)).toList
            x
          case op: ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator =>
            val res = op.toList
            res.foreach(b => if (b.size > RelOperator.blockSize) throw new AssertionError("Block size exceeded"))
            res.flatten
          case op: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator => op.execute().transpose.toList
          case op: early.volcano.Operator => op.toList
        },
        operator.getRowType
      )
    }, "Execution time")
    println("Total accesses: " + RelOperatorUtilLog.getFieldAccessCount)
    tmp
  }

  private def convert(rel: RelNode) = {
    val getAsStore = (s : String) => {
      s match {
        case "rowstore" => ((s: ScannableTable) => s.getAsRowStore)
        case "columnstore" =>((s: ScannableTable) => s.getAsColumnStore)
        case "paxstore" =>((s: ScannableTable) => s.getAsPAXStore)
        case _ => ((s: ScannableTable) => s.getAsRowStore)
      }
    }

    rel.accept(new RelShuttle() {
      override def visit(scan: TableScan): RelNode = factory.SCAN_FACTORY.createScan(scan.getCluster, scan.getTable, getAsStore(storeType))

      override def visit(scan: TableFunctionScan): RelNode = ???

      override def visit(values: LogicalValues): RelNode = ???

      override def visit(filter: LogicalFilter): RelNode = factory.FILTER_FACTORY.createFilter(filter.getInput.accept(this), filter.getCondition, filter.getVariablesSet)

      override def visit(project: LogicalProject): RelNode = factory.PROJECT_FACTORY.createProject(project.getInput.accept(this), project.getChildExps, project.getNamedProjects.asScala.map(e => e.right).asJava)

      override def visit(join: LogicalJoin): RelNode = {
        factory.JOIN_FACTORY.createJoin(join.getLeft.accept(this), join.getRight.accept(this), join.getCondition, join.getVariablesSet, join.getJoinType, join.isSemiJoinDone)
      }

      override def visit(correlate: LogicalCorrelate): RelNode = {
        visit(correlate.asInstanceOf[RelNode])
      }

      override def visit(union: LogicalUnion): RelNode = ???

      override def visit(intersect: LogicalIntersect): RelNode = ???

      override def visit(minus: LogicalMinus): RelNode = ???

      override def visit(aggregate: LogicalAggregate): RelNode = factory.AGGREGATE_FACTORY.createAggregate(aggregate.getInput.accept(this), aggregate.getGroupSet, aggregate.getGroupSets, aggregate.getAggCallList)

      override def visit(m: LogicalMatch): RelNode = ???

      override def visit(sort: LogicalSort): RelNode = factory.SORT_FACTORY.createSort(sort.getInput.accept(this), sort.getCollation, sort.offset, sort.fetch)

      override def visit(exchange: LogicalExchange): RelNode = ???

      override def visit(other: RelNode): RelNode = {
        println("Under conversion tree:")
        PrintUtil.printTree(rel)
        throw new IllegalArgumentException("Unexpected RelNode: " + other)
      }
    })
  }
}
