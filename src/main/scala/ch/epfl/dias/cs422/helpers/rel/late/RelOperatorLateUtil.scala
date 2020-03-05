package ch.epfl.dias.cs422.helpers.rel.late

import java.lang.reflect.Type

import ch.epfl.dias.cs422.helpers.rex.{RexExecutor, TupleContext}
import org.apache.calcite.DataContext
import org.apache.calcite.adapter.enumerable.RexToLixTranslator
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.linq4j.tree.{BlockBuilder, Expression, Expressions}
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexNode, RexProgramBuilder}
import org.apache.calcite.sql.validate.SqlConformanceEnum

import scala.jdk.CollectionConverters._

class LazyEvaluator private[helpers](val e: IndexedSeq[RexNode],
                                     val getter: LazyDataContextInputGetter,
                                     val tFactory: LazyDataContextInputGetter => IndexedSeq[Any] => IndexedSeq[Any])
  extends LazyEvaluatorRoot {
  lazy val t: IndexedSeq[Any] => IndexedSeq[Any] = {
    assert(getter != null)
    tFactory(getter)
  }

  override def apply(vs: IndexedSeq[Any]): IndexedSeq[Any] = t(vs)
}


class LazyDataContextInputGetter(rexBuilder: RexBuilder, val rowType: RelDataType, typeFactory: JavaTypeFactory, val evaluators: LazyEvaluatorRoot) extends RexToLixTranslator.InputGetter {
  class CodeGenLazyDataContextInputGetter(val getter: LazyDataContextInputGetter, val expression: Expression) extends RexToLixTranslator.InputGetter {
    override def field(list: BlockBuilder, index: Int, storageType: Type): Expression = getter.field(list, index, storageType, expression)
  }

  override def field(list: BlockBuilder, index: Int, storageType: Type): Expression = {
    field(list, index, storageType, RexToLixTranslator.convert(DataContext.ROOT, classOf[LazyTupleContext]))
  }

  def field(list: BlockBuilder, index: Int, storageType: Type, context: Expression): Expression = {
    evaluators match {
      case evaluator: LazyEvaluator =>
        val innerRowType = evaluator.getter.rowType
        val program = new RexProgramBuilder(innerRowType, rexBuilder)
        program.addProject(evaluator.e(index), "c" + this)

        RexToLixTranslator.translateProjects(program.getProgram, typeFactory, SqlConformanceEnum.DEFAULT, list, null, DataContext.ROOT, new CodeGenLazyDataContextInputGetter(evaluator.getter, context), null).get(0)
      case evaluator: MergeLazyEvaluator =>
        val ri = if (index >= evaluator.rowTypes.head.getFieldCount) 1 else 0
        val row_type_todo = evaluator.rowTypes(ri)
        val getter = new LazyDataContextInputGetter(rexBuilder, row_type_todo, typeFactory, if (ri == 0) evaluator.levs else evaluator.revs)

        val methodApply = classOf[LazyTupleContext].getMethod("apply", classOf[Int])
        val innerCtx = Expressions.call(context, methodApply, Expressions.constant(ri))

        getter.field(list, index - evaluator.rowTypes.head.getFieldCount * ri, storageType, innerCtx)
      case e: LazyEvaluatorAccess =>
        val methodApply = classOf[LazyTupleContext].getMethod("field", classOf[Int])
        LazyTupleContextRegistry.registry = e.l(index) :: LazyTupleContextRegistry.registry

        val recFromCtx = Expressions.call(context, methodApply, Expressions.constant(LazyTupleContextRegistry.registry.size - 1))
        val recordAccess = recFromCtx
        var sType = storageType
        if (sType == null) {
          val fieldType = rowType.getFieldList.get(index).getType
          sType = typeFactory.getJavaClass(fieldType)
        }
        RexToLixTranslator.convert(recordAccess, sType)
    }
  }
}

abstract class LazyEvaluatorRoot {
  def apply(vs: IndexedSeq[Any]): IndexedSeq[Any]
}

object LazyTupleContextRegistry {
  var registry: List[Long => Any] = Nil
}

class LazyTupleContext(val tuple: IndexedSeq[Any]) extends TupleContext {
  def apply(index: Int): LazyTupleContext = new LazyTupleContext(tuple(index).asInstanceOf[IndexedSeq[Any]])

  override def field(index: Int): Any = LazyTupleContextRegistry.registry(LazyTupleContextRegistry.registry.size - 1 - index)(tuple.head.asInstanceOf[Long])
}

private[helpers] class MergeLazyEvaluator(val levs: LazyEvaluatorRoot, val revs: LazyEvaluatorRoot, val rowTypes: List[RelDataType]) extends LazyEvaluatorRoot {
  override def apply(vs: IndexedSeq[Any]): IndexedSeq[Any] = {
    IndexedSeq(levs, revs).zip(vs).flatMap(f => f._1(f._2.asInstanceOf[IndexedSeq[Any]]))
  }
}

class Evaluator private[helpers](val tFactory: () => IndexedSeq[Any] => Any) {
  lazy val t: IndexedSeq[Any] => Any = tFactory()

  def apply(vs: IndexedSeq[Any]): Any = t(vs)
}


protected[helpers] object RelOperatorLateUtil {
  type VID = Long
  type VIDs = IndexedSeq[Any] // Either[VID, List[VIDs]]

  def lazyEval(cluster: RelOptCluster, e: IndexedSeq[RexNode], inputRowType: RelDataType, evaluators: LazyEvaluatorRoot): LazyEvaluator = {
    new LazyEvaluator(
      e,
      new LazyDataContextInputGetter(cluster.getRexBuilder, inputRowType, new JavaTypeFactoryImpl(), evaluators),
      (g: RexToLixTranslator.InputGetter) => {
        val fun = cluster.getPlanner.getExecutor.asInstanceOf[RexExecutor].getExecutable(
          cluster.getRexBuilder,
          e.asJava,
          inputRowType,
          (_: RelDataType, _: JavaTypeFactory) => g
        ).getFunction
        tuple: VIDs => fun.apply(new LazyTupleContext(tuple))
      }
    )
  }

  def eval(cluster: RelOptCluster, e: RexNode, inputRowType: RelDataType, evaluators: LazyEvaluatorRoot): Evaluator = {
    new Evaluator(
      () => {
        val getter = new LazyDataContextInputGetter(cluster.getRexBuilder, inputRowType, new JavaTypeFactoryImpl(), evaluators)

        val fun = cluster.getPlanner.getExecutor.asInstanceOf[RexExecutor].getExecutable(
          cluster.getRexBuilder,
          List(e).asJava,
          inputRowType,
          (_: RelDataType, _: JavaTypeFactory) => getter
        ).getFunction

        tuple: VIDs => fun.apply(new LazyTupleContext(tuple)).toIndexedSeq(0)
      })
  }
}
