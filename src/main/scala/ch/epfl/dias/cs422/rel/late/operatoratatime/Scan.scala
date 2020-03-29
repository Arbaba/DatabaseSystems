package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import ch.epfl.dias.cs422.helpers.store._

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  val store = tableToStore(table.unwrap(classOf[ScannableTable]))
  val vids = (0.toLong until store.getRowCount).map(x => IndexedSeq(x))
  var tuplesperpage= -1
  override def execute(): IndexedSeq[Column] =  vids

  private lazy val evals = new LazyEvaluatorAccess((0 until table.getColumnStrategies.size() ).map{col => row :Long=> {
      getElement(row, col)
  }}.toList)

  private def getElement(row: Long, col: Long) = store match {
    case cols: ColumnStore => cols.getColumn(col.toInt)(row.toInt)
    case rows: RowStore => rows.getRow(row.toInt)(col.toInt)
    case pax: PAXStore =>
      if(tuplesperpage < 0 ) {
        tuplesperpage = pax.getPAXPage(0)(0).length
      }
      val minipage = pax.getPAXPage(row.toInt / tuplesperpage)
      minipage(col.toInt)(row.toInt % tuplesperpage)
  }
  override def evaluators(): LazyEvaluatorAccess = evals
}
