package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    def tupleKeys(tuple: Tuple, keys: IndexedSeq[Int]) =  for(k <- keys)yield tuple(k)
    def tupleHash(tuple: Tuple, keys: IndexedSeq[Int]) =  tupleKeys(tuple, keys).hashCode()
    val hashTable : Map[Int, Tuple]= Map()
    val leftKeys = getLeftKeys
    var rightKeys = getRightKeys

    def asRows(d: IndexedSeq[Column]) = for(row <- (0 until d(0).length)) yield (0 until d.length ).map(col => d(col)(row))
    def asCols(d: IndexedSeq[Tuple]) = for(col <- (0 until d(0).length)) yield (0 until d.length ).map(row => d(row)(col))
    val ldata = asRows(left.execute())
    var rdata = asRows(right.execute())
    for(l <-ldata ){
      hashTable += tupleHash(l, getLeftKeys) -> l
    }
    val join = for(r <- rdata ) yield  (hashTable.get(tupleHash(r, rightKeys)) ++ r).toIndexedSeq
    asCols(join)
  }
}
