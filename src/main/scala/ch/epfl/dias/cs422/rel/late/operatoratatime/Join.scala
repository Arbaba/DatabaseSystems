package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorRoot
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val leftVIDS = left.execute()
    val rightVIDS = right.execute()

    val tmpl = leftVIDS.map(x => left.evaluators()(x))
    var tmpr = rightVIDS.map(x => right.evaluators()(x))
    implicit def anyflattener[A](a: A) : Iterable[A] = Some(a)

    def tupleKeys(tuple: Tuple, keys: IndexedSeq[Int]) =  for(k <- keys)yield tuple(k)
    def tupleHash(tuple: Tuple, keys: IndexedSeq[Int]) =  tupleKeys(tuple, keys).hashCode()
    val hashTable : Map[Int, IndexedSeq[Any]]= Map()
    if(tmpl.isEmpty || tmpr.isEmpty){
      IndexedSeq()
    }else {
      val ldata = tmpl.asInstanceOf[IndexedSeq[Column]]
      var rdata = tmpr
      for((l,vid) <-ldata.zip(leftVIDS) ){
        val key = tupleHash(l, getLeftKeys)
        hashTable.get(key) match {
          case Some(list ) => hashTable.update(key, (vid) +: list)
          case None => hashTable += key -> IndexedSeq(vid)
        }
      }
      val join = for((r, vid) <- rdata.zip(rightVIDS); tuples <- hashTable.get(tupleHash(r, getRightKeys))) yield  tuples.map{case (list)=> IndexedSeq(list ,(vid)) }

      if(join.isEmpty){
        IndexedSeq()

      }else {
        join.flatten
        //IndexedSeq(IndexedSeq(IndexedSeq(0.toLong),IndexedSeq(0.toLong)))
      }
    }
  }

  private lazy val evals = lazyEval(left.evaluators(), right.evaluators(), left.getRowType, right.getRowType)

  override def evaluators(): LazyEvaluatorRoot = evals
}
