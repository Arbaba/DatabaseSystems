package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.RexNode

import scala.collection.JavaConverters._
import scala.collection.mutable.SortedSet
class Wrapper (tuple: Tuple, collation: RelCollation) extends Ordered[Wrapper]{
  def compare(that: Wrapper): Int = {
    recCompare(that, collation.getFieldCollations.asScala.toSeq)
  }
  def getTuple: Tuple = tuple

  def get(i:Int) = tuple(i)
  def recCompare(that: Wrapper, comparators :Seq[RelFieldCollation]): Int = comparators match{
    case Seq() => 0
    case Seq(head: RelFieldCollation, tail@_*) =>
      val idx = head.getFieldIndex()
      val (a, b) = (get(idx).asInstanceOf[Comparable[Any]], that.get(idx).asInstanceOf[Comparable[Any]])
      (head.direction match {
        case Direction.ASCENDING =>
          a.compareTo(b)
        case Direction.DESCENDING =>
          b.compareTo(a)
        case Direction.STRICTLY_DESCENDING =>
          ???
        case Direction.STRICTLY_ASCENDING =>
          ???
        case _ => throw new Exception("Unknown sorting direction")
      }) match {
        case 0 => recCompare(that, tail)
        case r => r
      }
  }
}

object Wrapper{
  def apply(tuple:Tuple, collation:RelCollation): Wrapper = new Wrapper(tuple, collation)
}


class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  var ndiscarded :Int = offset match {
    case null  => 0
    case _ => evalLiteral(offset).asInstanceOf[Int]
  }

  var nfetch :Int = fetch match {
    case null  => Int.MaxValue
    case _ => evalLiteral(fetch).asInstanceOf[Int]
  }
  var data : SortedSet[Wrapper] = SortedSet()
  var it :Iterator[Wrapper]= data.iterator
  override def open(): Unit = {
    for(tuple <-input.iterator.toList.drop(ndiscarded).take(nfetch)){
      println(tuple)
      data+= Wrapper(tuple, collation)
    }
    it = data.iterator

  }

  override def next(): Tuple = {
    if(it.hasNext ){
      val t = it.next().getTuple
      t
    }else {
      null
    }
  }

  override def close(): Unit = input.close()
}

