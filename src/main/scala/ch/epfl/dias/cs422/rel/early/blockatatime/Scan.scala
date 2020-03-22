package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.store._
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected lazy val store: Store = tableToStore(table.unwrap(classOf[ScannableTable]))
  var index = 0
  override def open(): Unit = {
    index = 0
  }

  override def next(): Block = {
    if(index >= table.getRowCount){
      null
    }else {
      def extractRow(index: Int) =  store match {
        case rows: RowStore =>
          rows.getRow(index)
        case cols: ColumnStore => (0 until table.getRowType.getFieldCount ).map(i => cols.getColumn(i)(index))
        case pax: PAXStore =>

          val tuplesperpage = pax.getPAXPage(0)(0).length
          val minipage = pax.getPAXPage(index / tuplesperpage)
          minipage.map(m =>m(index % tuplesperpage))
      }

      val tuples = for(i <- (index until Math.min(index + 4, table.getRowCount().toInt)))yield extractRow(i)
      index += 4
      tuples
    }
  }


  override def close(): Unit = {


  }
}

object Helper{

  def tupleKeys(tuple: Tuple, keys: IndexedSeq[Int]) =  for(k <- keys)yield tuple(k)
  def tupleHash(tuple: Tuple, keys: IndexedSeq[Int]) =  tupleKeys(tuple, keys).hashCode()

}

class Buffer(blockSize : Int) {
  var buffer :Block = IndexedSeq()
  var foundNull = false
  def flush():Block = {
    val tmp = buffer.take(blockSize)
    buffer = buffer.drop(blockSize)
    tmp
  }

  def register(ts:Block)= {
    buffer = buffer ++ ts
  }

  def fillBuffer(blockfetcher: Unit => Block, processing:  Block => Block): Unit = {
    if(buffer.length <= blockSize && ! foundNull){
      val v = blockfetcher(())
      v match {

        case t:Block =>
          register(processing(v))
          if(buffer.isEmpty)
          fillBuffer(blockfetcher, processing)
        case _ =>
          foundNull = true

      }
    }
  }
}
