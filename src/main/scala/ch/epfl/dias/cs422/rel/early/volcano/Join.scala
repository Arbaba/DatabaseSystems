package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode
import scala.collection.mutable.Map
class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  val hashTable : Map[Int, Tuple]= Map()
  var rindex :Int = 0
  val leftKeys = getLeftKeys
  var rightKeys = getRightKeys

  def tupleKeys(tuple: Tuple, keys: IndexedSeq[Int]) =  for(k <- keys)yield tuple(k)
  def tupleHash(tuple: Tuple, keys: IndexedSeq[Int]) =  tupleKeys(tuple, keys).hashCode()

  override def open(): Unit = {

    left.open()
    for(l <-left.iterator ){
      hashTable += tupleHash(l, getLeftKeys) -> l
    }
    println(hashTable)
    right.open()
  }

  override def next(): Tuple = {
    right.next() match {
      case null =>
        println("close")

        null
      case r =>
        hashTable.get(tupleHash(r, rightKeys)) match {
          case Some(l) =>
            println("join")
            l ++ r
          case _ => next()
        }
    }

  }

  override def close(): Unit = {
    left.close()
    right.close()
  }
}
