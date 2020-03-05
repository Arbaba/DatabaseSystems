package ch.epfl.dias.cs422.helpers.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column

trait Operator extends RelOperator with Iterable[Column] {
  def execute(): IndexedSeq[Column]

  final def iterator: Iterator[Column] = execute().iterator
}
