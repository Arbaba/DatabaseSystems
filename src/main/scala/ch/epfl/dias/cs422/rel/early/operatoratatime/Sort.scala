package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode
import scala.jdk.CollectionConverters._

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  override def execute(): IndexedSeq[Column] = {
    var ndiscarded :Int = offset match {
      case null  => 0
      case _ => evalLiteral(offset).asInstanceOf[Int]
    }

    var nfetch :Int = fetch match {
      case null  => Int.MaxValue
      case _ => evalLiteral(fetch).asInstanceOf[Int]
    }

    sort(input.execute().drop(ndiscarded).take(nfetch), collation.getFieldCollations.asScala.toSeq)
  }
  def sort(data: IndexedSeq[Column], sorters : Seq[RelFieldCollation]): IndexedSeq[Column] = {
        data match {
          case IndexedSeq() => data
          case _ =>
            val nrows  = data(0).length
            val sortedIdx = (0 until nrows).sortWith((a,b) => {
              val c = compare(getTuple(data, a), getTuple(data, b), sorters)
              c < 0
            })

          data.map(col => sortedIdx.map(i => col(i)))
        }


  }
  def compare(a: Tuple, b: Tuple, sorters: Seq[RelFieldCollation]): Int = {
    sorters match {
      case Seq() => 0
      case Seq(head: RelFieldCollation, tail@_*) =>
        val idx = head.getFieldIndex()
        val (x, y) = (a(idx).asInstanceOf[Comparable[Any]], b(idx).asInstanceOf[Comparable[Any]])
        head.direction match {
          case Direction.ASCENDING =>
            x.compareTo(y)
          case Direction.DESCENDING =>
            y.compareTo(x)
          case _ => throw new Exception("Unhandled comparison direction")
        }
    }
  }
  implicit def anyflattener[A](a: A) : Iterable[A] = Some(a)
  def asCols(d: IndexedSeq[Tuple]) = for (col <- (0 until d(0).length)) yield ((0 until d.length).map(row => d(row)(col))).flatten

  def getTuple(data: IndexedSeq[Column], i: Int): Tuple = data.map{col : Column => col(i)}.flatten

}
