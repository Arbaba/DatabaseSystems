package ch.epfl.dias.cs422.helpers.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}

trait Operator extends RelOperator with Iterable[Block] {
  final val blockSize: Int = RelOperator.blockSize

  /** Resets Operator to the initial state.
   *
   * Called exactly once before any calls to {::next()} function.
   */
  def open(): Unit

  /** Returns next tuple produced by the operator.
   *
   * Return a tuple as a List of attributes, or Nil to signify that there
   * are no more available tuples.
   * The order and type of the List elements conforms to getRowType.
   * After next() returns Nil, calling next() again has undefined behavior
   * and the Operator may logically enter a state that only accepts calls
   * to close() and open().
   *
   * @return Next tuple as a List of attributes, or Nil for end of stream.
   */
  def next(): Block

  /** Allows the Operator to clean up its resources.
   *
   * Every call to open() should be matched to a call to open() and after
   * a call to close(), the Operator will only accept calls to open().
   * Calling next() after close(), without re-open()ing the operator,
   * has undefined behavior.
   */
  def close(): Unit

  /** Returns an iterator to iterate over the tuples produced by the operator.
   *
   * The iterator takes care of opening and closing the operator.
   *
   * @return Iterator over the tuples produced by the current operator.
   */
  final override def iterator: Iterator[Block] = new Iterator[Block] with AutoCloseable {
    private val op = Operator.this.copy(getTraitSet, getInputs).asInstanceOf[Operator]
    op.open()

    // States:
    //  n is empty               : op MAY have more items
    //  n is non-empty, n != Nil : item already prepared and not pushed
    //  n is non-empty, n == Nil : op closed
    var n: Option[List[Tuple]] = Option.empty

    def prepareNext(): Unit = {
      if (n.nonEmpty) return
      val nxt = op.next()
      if (nxt == null)
        n = Option(Nil)
      else
        n = Option(nxt.toList)
    }

    override def hasNext: Boolean = {
      prepareNext()
      n.get != Nil
    }

    override def next(): Block = {
      prepareNext()
      val ret = n.get
      if (ret != Nil) n = Option.empty
      ret.toIndexedSeq
    }

    override def close(): Unit = {
      op.close()
    }
  }
}
