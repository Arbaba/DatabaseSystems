package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    println(condition.toString())
    def tupleKeys(tuple: Tuple, keys: IndexedSeq[Int]) =  for(k <- keys)yield tuple(k)
    def tupleHash(tuple: Tuple, keys: IndexedSeq[Int]) =  tupleKeys(tuple, keys).hashCode()
    val hashTable : Map[Int, IndexedSeq[Tuple]]= Map()
    val leftKeys = getLeftKeys
    var rightKeys = getRightKeys
    implicit def anyflattener[A](a: A) : Iterable[A] = Some(a)

    def asRows(d: IndexedSeq[Column]) = for (row <- (0 until d(0).length)) yield ((0 until d.length).map(col => d(col)(row))).flatten

    def asCols(d: IndexedSeq[Tuple]) = for (col <- (0 until d(0).length)) yield ((0 until d.length).map(row => d(row)(col))).flatten

    val tmpl = left.execute()
    val tmpr = right.execute()
    if(tmpl.isEmpty || tmpr.isEmpty){
      IndexedSeq()
    }else {
      println("l")
      val ldata = asRows(tmpl)
      println("r")
      var rdata = asRows(tmpr)
      for(l <-ldata ){
        val key = tupleHash(l, getLeftKeys)
        hashTable.get(key) match {
          case Some(list ) => hashTable.update(key, l +: list)
          case None => hashTable += key -> IndexedSeq(l)
        }
      }
      val join = for(r <- rdata; tuples <- hashTable.get(tupleHash(r, rightKeys))) yield  tuples.map{case (list)=> list ++ r}

      if(join.isEmpty){
        IndexedSeq()

      }else {
        asCols(join.flatten)
      }
    }

  }
}
