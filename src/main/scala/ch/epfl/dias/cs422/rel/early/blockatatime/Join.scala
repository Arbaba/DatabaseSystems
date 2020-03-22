package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  var buffer: Buffer = new Buffer(blockSize)

  val hashTable : Map[Int, List[Tuple]]= Map()
  var rindex :Int = 0
  val leftKeys = getLeftKeys
  var rightKeys = getRightKeys

  def tupleKeys(tuple: Tuple, keys: IndexedSeq[Int]) =  for(k <- keys)yield tuple(k)
  def tupleHash(tuple: Tuple, keys: IndexedSeq[Int]) =  tupleKeys(tuple, keys).hashCode()


  override def open(): Unit = {
     buffer = new Buffer(blockSize)
    var i = 0
    val data = left.iterator.toList.flatten
    for(l <-data){
      i += 1
      println("Join " + condition + " " + l )
      hashTable.get(tupleHash(l, getLeftKeys)) match {
        case Some(list) =>hashTable += tupleHash(l, getLeftKeys) -> (l :: list)
        case None =>      hashTable += tupleHash(l, getLeftKeys) -> List(l)

      }
    }
    right.open()

  }

  override def next(): Block = {
    val fetchNext :Unit => Block = _ => {
      right.next()
    }
    buffer.fillBuffer(fetchNext, i => processRightBlock(i))
    val data = buffer.flush()
    println("buff " + data)

    if(data.isEmpty){
      null
    }else {
      data
    }
  }

  override def close(): Unit = {
    left.close()
    right.close()
  }


  def processRightBlock(rright: Block):Block = {
    rright match {
      case null =>

        IndexedSeq()
      case rs =>
        val t = rs.flatMap(r => {
          hashTable.get(tupleHash(r, rightKeys)) match {
            case Some(tuples) =>
              Some(tuples.map(t => t ++ r).toIndexedSeq)
            case _ => None

          }
        }).flatten
        println("rs " + t)

        println(condition + " " +t)
        t

    }
  }
}
