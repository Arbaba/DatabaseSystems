package ch.epfl.dias.cs422.helpers.rel

import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Elem, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexNode, RexUtil}

object RelOperator{
  final type Elem = Any
  final type Tuple = IndexedSeq[Elem]
  final type Column = IndexedSeq[Elem]
  final type Block = IndexedSeq[Tuple]
  final type PAXMinipage = IndexedSeq[Elem] // like a column but with maximum length of X elements
  final type PAXPage = IndexedSeq[PAXMinipage]

  final val blockSize: Int = 4
}

trait RelOperator extends RelNode {
  /** Produces a function that evaluates a RexNode on a tuple.
   *
   * Produces the results of expression @p e, applied to an input tuple @p input.
   * Example usage:
   *  eval(cond, inputTupleType)(tuple)
   *
   * @param e            Tuple-level expression to evaluate
   * @param inputRowType Type of expected input tuple for the expression evaluation
   * @return Returns a function that accepts a tuple as and returns the result of expression @p e.
   */
  final def eval(e: RexNode, inputRowType: RelDataType): Tuple => Elem = {
    RelOperatorUtil.eval(getCluster, e, inputRowType)
  }

  final def eval(e: IndexedSeq[RexNode], inputRowType: RelDataType): Tuple => Tuple = {
    RelOperatorUtil.eval(getCluster, e, inputRowType)
  }

  final def evalLiteral(e: RexNode): Any = {
    assert(RexUtil.isLiteral(e, true), "evalLiteral only accepts literals")
    RelOperatorUtil.eval(getCluster, e, getRowType /* we just need some type, doesn't matter which */)(null)
  }

  final def aggReduce(e1: Elem, e2: Elem, aggCall: AggregateCall): Elem = aggCall.reduce(e1, e2)

  final def aggEmptyValue(aggCall: AggregateCall): Elem = aggCall.emptyValue
}
