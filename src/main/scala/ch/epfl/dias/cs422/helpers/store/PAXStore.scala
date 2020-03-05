package ch.epfl.dias.cs422.helpers.store

import ch.epfl.dias.cs422.helpers.rel.RelOperator.PAXPage
import ch.epfl.dias.cs422.helpers.rel.RelOperatorUtilLog

class PAXStore private [store] (private val data: IndexedSeq[PAXPage], private val rowCount: Long) extends Store {
  def getPAXPage(i: Integer): PAXPage = {
    val tmp = data(i)
    if (tmp.isEmpty) {
      RelOperatorUtilLog.accesses = RelOperatorUtilLog.accesses + tmp.size * tmp.head.size
    }
    tmp
  }

  override def getRowCount: Long = rowCount
}
