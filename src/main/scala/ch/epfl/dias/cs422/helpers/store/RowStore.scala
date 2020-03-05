package ch.epfl.dias.cs422.helpers.store

import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.RelOperatorUtilLog

class RowStore private [store] (private val data: IndexedSeq[Tuple]) extends Store {
  def getRow(i: Int): Tuple = {
    val tmp = data(i)
    RelOperatorUtilLog.accesses = RelOperatorUtilLog.accesses + tmp.size
    tmp
  }

  def getRowCount: Long = data.size
}
