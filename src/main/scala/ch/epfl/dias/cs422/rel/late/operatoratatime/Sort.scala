package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  val vids : IndexedSeq[Column] =input.execute()
  override def execute(): IndexedSeq[Column] = {

    var ndiscarded :Int = offset match {
      case null  => 0
      case _ => evalLiteral(offset).asInstanceOf[Int]
    }

    var nfetch :Int = fetch match {
      case null  => Int.MaxValue
      case _ => evalLiteral(fetch).asInstanceOf[Int]
    }

    (0 until vids.drop(ndiscarded).take(nfetch).length).map(x => IndexedSeq(x.toLong))
  }
  def sort(data: IndexedSeq[Tuple], sorters : Seq[RelFieldCollation]): IndexedSeq[Column] = {
    data match {
      case IndexedSeq() => data
      case _ =>
        val nrows  = data.length
        val sortedIdx = (0 until nrows).sortWith((a,b) => {
          val c = compare(data(a),data(b), sorters)
          c < 0
        })

       sortedIdx.map(i => data(i))
    }


  }
  def compare(a: Tuple, b: Tuple, sorters: Seq[RelFieldCollation]): Int = {
    sorters match {
      case Seq() => 0
      case Seq(head: RelFieldCollation, tail@_*) =>
        val idx = head.getFieldIndex()
        val (x, y) = (a(idx).asInstanceOf[Comparable[Any]], b(idx).asInstanceOf[Comparable[Any]])
        val cmp = head.direction match {
          case Direction.ASCENDING =>
            x.compareTo(y)

          case Direction.DESCENDING =>
            y.compareTo(x)
          case _ => throw new Exception("Unhandled comparison direction")
        }
        if(cmp == 0)
          compare(a,b, sorters.tail)
        else cmp
    }
  }

  private lazy val evals = {
    val data = sort(vids.map(x => input.evaluators()(x)), collation.getFieldCollations.asScala.toSeq)
    new LazyEvaluatorAccess((0 until input.getRowType.getFieldCount).map{col => row: Long =>
      data(row.toInt)(col)
    }.toList)
  }

  override def evaluators(): LazyEvaluatorAccess = evals
}
