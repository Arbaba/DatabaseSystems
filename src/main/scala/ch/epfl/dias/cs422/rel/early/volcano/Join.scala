package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode

import scala.annotation.tailrec
import scala.collection.mutable.Map
class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  val hashTable : Map[Int, List[Tuple]]= Map()
  var rindex :Int = 0
  val leftKeys = getLeftKeys
  var rightKeys = getRightKeys

  def tupleKeys(tuple: Tuple, keys: IndexedSeq[Int]) =  for(k <- keys)yield tuple(k)
  def tupleHash(tuple: Tuple, keys: IndexedSeq[Int]) =  tupleKeys(tuple, keys).hashCode()

  override def open(): Unit = {

    println("open" + hashTable.size)
    var i = 0
    val data = left.iterator.toList
    for(l <-data){
      i += 1
      println(i)
      hashTable.get(tupleHash(l, getLeftKeys)) match {
        case Some(list) =>hashTable += tupleHash(l, getLeftKeys) -> (l :: list)
        case None =>      hashTable += tupleHash(l, getLeftKeys) -> List(l)

      }
    }
    right.open()
  }
  var bufferedResult : Seq[Tuple] = IndexedSeq()
  var currentRight : Tuple = IndexedSeq()
  override def next(): Tuple = {
    @tailrec
    def tailrecNext(): Any = {
      right.next() match {
        case null =>

          null
        case r =>
          hashTable.get(tupleHash(r, rightKeys)) match {
            case Some(tuples) =>
              bufferedResult = tuples
              currentRight = r
            case _ =>
              println(r)
              tailrecNext()
          }
      }
    }

    bufferedResult match {
      case Seq() => tailrecNext() match {
        case null => null
        case _  => next()
      }
      case Seq(head, tail@_*) =>
        val  tmp = head ++ currentRight
        bufferedResult = tail
        tmp
    }

  }

  override def close(): Unit = {
    left.close()
    right.close()
  }
}
