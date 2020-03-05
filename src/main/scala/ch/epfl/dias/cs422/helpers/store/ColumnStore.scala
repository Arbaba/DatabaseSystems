package ch.epfl.dias.cs422.helpers.store

import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.RelOperatorUtilLog

class ColumnStore private [store] (private val data: IndexedSeq[Column], private val rowCount: Long) extends Store {
  def getColumn(i: Int): Column = {
    val tmp = data(i)
    RelOperatorUtilLog.accesses = RelOperatorUtilLog.accesses + tmp.size
    tmp
  }

  override def getRowCount: Long = rowCount // otherwise we can't retrieve it for 0-column tables
}
