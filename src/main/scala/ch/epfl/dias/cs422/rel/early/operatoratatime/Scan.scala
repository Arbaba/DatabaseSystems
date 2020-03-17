package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))
    val ncols = table.getColumnStrategies.size()
    def asRows(d: IndexedSeq[Column]) = for(row <- (0 until d(0).length)) yield (0 until d.length ).map(col => d(col)(row))
    def asCols(d: IndexedSeq[Tuple]) = for(col <- (0 until d(0).length)) yield (0 until d.length ).map(row => d(row)(col))
    store match  {
      case rows: RowStore =>
        if(rows.getRowCount == 0){ IndexedSeq()
        }else{
          val allRows = for(i <- 1 until rows.getRowCount.toInt )yield rows.getRow(i)
          asCols(allRows)
        }

      case cols: ColumnStore =>
        if(cols.getRowCount ==0){
          IndexedSeq()
        }else{
          for(i <- 0 until ncols)yield  cols.getColumn(i)

        }

      case pax: PAXStore =>
        if(pax.getRowCount == 0){
          IndexedSeq()
        }else {
          val tuplesperpage = pax.getPAXPage(0)(0).length

          for(col <-  0 until ncols) yield {for(row <- 0 until pax.getRowCount.toInt) yield pax.getPAXPage(row / tuplesperpage)(col)}
        }

    }
  }
}
