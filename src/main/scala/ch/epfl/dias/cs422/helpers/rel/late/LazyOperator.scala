package ch.epfl.dias.cs422.helpers.rel.late

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexInputRef, RexNode}

import scala.jdk.CollectionConverters._

class LazyEvaluatorAccess(val l: List[Long => Any]) extends LazyEvaluatorRoot {
  def apply(vs: IndexedSeq[Any]): IndexedSeq[Any] = {
    assert(vs.size == 1)
    l.toIndexedSeq.map(f => f(vs.head.asInstanceOf[Long]))
  }

  def apply(vs: Long): IndexedSeq[Any] = {
    l.toIndexedSeq.map(f => f(vs))
  }
}

trait LazyOperator extends RelNode {
  type VID = RelOperatorLateUtil.VID
  type VIDs = RelOperatorLateUtil.VIDs

  /*
   * Creates an evaluators for expression e. The input row conforms to type inputRowType and is constructed through
   * the evaluators.
   * The expected VID input is the same as the input for the evaluators.
   *
   * See LazyEva
   */
  def eval(e: RexNode, inputRowType: RelDataType, evaluators: LazyEvaluatorRoot): Evaluator = {
    RelOperatorLateUtil.eval(getCluster, e, inputRowType, evaluators)
  }

  /**
   * Given a LazyEvaluatorRoot that results on a row type rowType, create a new lazy expression that only evaluates
   * the fields pointed by the indices in the ind array
   */
  def indices(ind: IndexedSeq[Int], rowType: RelDataType, eval: LazyEvaluatorRoot): LazyEvaluator = {
    val lk = ind.map(i => new RexInputRef(i, rowType.getFieldList.get(i).getType))
    lazyEval(lk, rowType, eval)
  }

  /**
   * Given a IndexedSeq of expressions, it produces the evaluators for them. The input is assumed to be comming form the
   * `evaluators` input and the evaluators should produce a row of inputRowType format.
   */
  def lazyEval(e: IndexedSeq[RexNode], inputRowType: RelDataType, evaluators: LazyEvaluatorRoot): LazyEvaluator = {
    RelOperatorLateUtil.lazyEval(getCluster, e, inputRowType, evaluators)
  }

  /**
   * Create a new evaluator that combines the left and right evaluators to produce a tuple. The new input is expected
   * to be an IndexedSeq with two VIDs, one for each side. (eg. IndexedSeq(5, 10) or IndexedSeq(IndexedSeq(5, 10), 25) )
   */
  def lazyEval(leftEval: LazyEvaluatorRoot, rightEval: LazyEvaluatorRoot, leftRowType: RelDataType, rightRowType: RelDataType): LazyEvaluator = {
    val rowTypeList = leftRowType.getFieldList.asScala ++ rightRowType.getFieldList.asScala
    val rowType = getCluster.getTypeFactory.createStructType(rowTypeList.map(f => f.getType).asJava, rowTypeList.map(f => f.getName).asJava)

    val fs = rowType.getFieldList.asScala.zipWithIndex.map(e => new RexInputRef(e._1.getIndex, e._1.getType)).toIndexedSeq
    lazyEval(fs, rowType, new MergeLazyEvaluator(leftEval, rightEval, List(leftRowType, rightRowType)))
  }

  def evaluators(): LazyEvaluatorRoot
}
