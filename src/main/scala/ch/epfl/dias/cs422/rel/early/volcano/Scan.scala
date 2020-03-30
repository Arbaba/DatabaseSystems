package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.store._
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected val scannable: Store = tableToStore(table.unwrap(classOf[ScannableTable]))
  protected var index: Int = 0
  protected var maxidx : Long = 0.toLong


  override def open(): Unit = {
    index = 0
    maxidx = scannable.getRowCount
  }

  override def next(): Tuple = {
    if(index >= maxidx){
      null
    } else {
      val tuple: Tuple = scannable match {
        case rows: RowStore => rows.getRow(index)
        case cols: ColumnStore => (0 until table.getRowType.getFieldCount ).map(i => cols.getColumn(i)(index))
        case pax: PAXStore =>

           val tuplesperpage = pax.getPAXPage(0)(0).length
           val minipage = pax.getPAXPage(index / tuplesperpage)
           minipage.map(m =>m(index % tuplesperpage))
      }
      index += 1
      tuple
    }

  }

  override def close(): Unit = ()
}
