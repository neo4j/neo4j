/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.MemoryTrackingEagerBatchingIterator.INIT_CHUNK_SIZE
import org.neo4j.function.Suppliers
import org.neo4j.io.IOUtils
import org.neo4j.kernel.impl.util.collection.EagerBuffer
import org.neo4j.kernel.impl.util.collection.EagerBuffer.createEagerBuffer
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.memory.Measurable
import org.neo4j.memory.MemoryTracker
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.AnyValue

import scala.collection.GenTraversableOnce
import scala.collection.Iterator.empty
import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Adds the method [[close]] over the normal [[Iterator]] interface.
 * Always calls close when exhausted automatically.
 * Allows to register resources to get closed as well.
 *
 * Overrides scala [[Iterator]] convenience functions to create new
 * [[ClosingIterator]]s that propagate [[close]] calls.
 */
abstract class ClosingIterator[+T] extends AutoCloseable {
  self =>

  /**
   * Resources that will be closed when this iterator gets closed.
   * We expect prepend-only, traverse once, small lists, thus we decided for
   * a List implementation here.
   */
  private var resources: java.util.ArrayDeque[AutoCloseable] = _
  private var closed = false

  // ABSTRACT METHODS
  /**
   * Perform additional actions on close
   */
  protected[this] def closeMore(): Unit
  def next(): T

  /**
   * Implements the test of whether this iterator can provide another element.
   */
  @inline
  protected[this] def innerHasNext: Boolean

  // PUBLIC API
  def isEmpty: Boolean = !hasNext
  def nonEmpty: Boolean = hasNext
  def toSeq: Seq[T] = toIterator.toSeq
  def toList: List[T] = toIterator.toList

  def toIterator: Iterator[T] = new Iterator[T] {
    override def hasNext: Boolean = self.hasNext
    override def next(): T = self.next()
  }

  def toArray[B >: T : ClassTag]: Array[B] = {
    val buffer = ArrayBuffer.empty[B]
    while (hasNext) {
      buffer += next()
    }
    buffer.toArray
  }
  def toSet[B >: T]: immutable.Set[B] = toIterator.toSet

  def foreach[U](f: T => U): Unit = {
    while (hasNext) {
      f(next())
    }
  }

  def size: Int = {
    var count = 0
    while (hasNext) {
      count += 1
      next()
    }
    count
  }

  /**
   * Adds a resource to the list of resources that are closed by this iterator.
   * @return the same instance
   */
  def closing(resource: AutoCloseable): self.type = {
    if (resource != self) {
      if (resources == null) {
        resources = new java.util.ArrayDeque[AutoCloseable](4) // We rarely add many resources to close
      }
      resources.addLast(resource)
    }
    self
  }

  /**
   * Close the iterator and all resources. Calls to [[hasNext]] or [[next()]] after this become undefined.
   */
  final def close(): Unit = {
    if (!closed) {
      if (resources != null) {
        IOUtils.closeAll(resources)
      }
      closeMore()
      closed = true
    }
  }

  final def hasNext: Boolean = {
    val _hasNext = innerHasNext
    if (!_hasNext) {
      close()
    }
    _hasNext
  }

  // CONVENIENCE METHODS

  // except for closing a copy of [[Iterator.flatMap]]
  def flatMap[B](f: T => ClosingIterator[B]): ClosingIterator[B] = new ClosingIterator[B] {
    private var cur: ClosingIterator[B] = ClosingIterator.empty

    private def nextCur(): Unit = {
      cur = f(self.next())
    }

    def innerHasNext: Boolean = {
      while (!cur.hasNext) {
        if (!self.hasNext) return false
        nextCur()
      }
      true
    }

    def next(): B = {
      if (hasNext) cur.next()
      else ClosingIterator.empty.next()
    }

    override def closeMore(): Unit = {
      self.close()
      cur match {
        case closingIterator: ClosingIterator[_] => closingIterator.close()
        case _                                   =>
      }
    }
  }

  def withFilter(p: T => Boolean): ClosingIterator[T] = filter(p)

  // except for closing a copy of [[Iterator.filter]]
  def filter(p: T => Boolean): ClosingIterator[T] = new ClosingIterator[T] {
    private var hd: T = _
    private var hdDefined: Boolean = false

    override protected[this] def innerHasNext: Boolean = hdDefined || {
      do {
        if (!self.hasNext) return false
        hd = self.next()
      } while (!p(hd))
      hdDefined = true
      true
    }

    def next(): T =
      if (hasNext) { hdDefined = false; hd }
      else empty.next()

    override protected[this] def closeMore(): Unit = self.close()
  }

  // except for closing a copy of [[Iterator.map]]
  def map[B](f: T => B): ClosingIterator[B] = new ClosingIterator[B] {
    override protected[this] def innerHasNext: Boolean = self.hasNext

    def next(): B = f(self.next())

    override protected[this] def closeMore(): Unit = self.close()
  }

  /**
   * Lazily concatenates two closing iterators.
   * 
   * Note!! If `that` is never materialised it will not be closed!
   */
  def addAllLazy[B >: T](that: () => ClosingIterator[B]): ClosingIterator[B] = new ClosingIterator[B] {
    // We read this into a lazy local variable here to avoid creating a new `that` iterator multiple times.
    // This is OK, since we expect to close both sides anyway.
    private val lazyThat = Suppliers.lazySingleton(() => that.apply())

    private var cur: ClosingIterator[B] = self

    override protected[this] def innerHasNext: Boolean = {
      if (cur.hasNext) {
        true
      } else if (cur eq self) {
        cur = lazyThat.get()
        cur.hasNext
      } else {
        false
      }
    }

    def next(): B = if (hasNext) cur.next() else ClosingIterator.empty.next()

    override protected[this] def closeMore(): Unit = {
      self.close()
      if (lazyThat.isInitialised) {
        lazyThat.get().close()
      }
    }
  }

  // NOTE: direct port of Iterator#collect except for closing the inner iterator
  def collect[B](pf: PartialFunction[T, B]): ClosingIterator[B] = new ClosingIterator[B] {
    // Manually buffer to avoid extra layer of wrapping with buffered
    private[this] var hd: T = _

    // Little state machine to keep track of where we are
    // Seek = 0; Found = 1; Empty = -1
    // Not in vals because scalac won't make them static (@inline def only works with -optimize)
    // BE REALLY CAREFUL TO KEEP COMMENTS AND NUMBERS IN SYNC!
    private[this] var status = 0 /*Seek*/

    override protected def innerHasNext: Boolean = {
      while (status == 0 /*Seek*/ ) {
        if (self.hasNext) {
          hd = self.next()
          if (pf.isDefinedAt(hd)) status = 1 /*Found*/
        } else status = -1 /*Empty*/
      }
      status == 1 /*Found*/
    }

    def next(): B =
      if (hasNext) { status = 0 /*Seek*/; pf(hd) }
      else ClosingIterator.empty.next()

    override protected[this] def closeMore(): Unit = {
      self.close()
    }
  }
}

object ClosingIterator {

  implicit class JavaAutoCloseableIteratorAsClosingIterator[T](val iterator: java.util.Iterator[T] with AutoCloseable) {
    def asSelfClosingIterator: ClosingIterator[T] = new DelegatingClosingJavaIterator(iterator).closing(iterator)
  }

  implicit class JavaIteratorAsClosingIterator[T](val iterator: java.util.Iterator[T]) {
    def asClosingIterator: ClosingIterator[T] = new DelegatingClosingJavaIterator(iterator)
  }

  implicit class JavaCollectionAsClosingIterator[T](val collection: java.util.Collection[T]) {
    def asClosingIterator: ClosingIterator[T] = new DelegatingClosingJavaIterator(collection.iterator())
  }

  implicit class ScalaSeqAsClosingIterator[T](val seq: GenTraversableOnce[T]) {
    def asClosingIterator: ClosingIterator[T] = new DelegatingClosingIterator(seq.toIterator)
  }

  implicit class OptionAsClosingIterator[T](val option: Option[T]) {
    def asClosingIterator: ClosingIterator[T] = new DelegatingClosingIterator(option.toIterator)
  }

  implicit class MemoryTrackingClosingIterator[T <: Measurable](val iterator: ClosingIterator[T]) {

    /**
     * Groups this iterator in eager, memory tracked batches of the specified size.
     * 
     * Note! Caller of next() is responsible to close the [[EagerBuffer]], it will not be closed by
     * the ClosingIterator.
     */
    def eagerGrouped(size: Long, memoryTracker: MemoryTracker): ClosingIterator[EagerBuffer[T]] = {
      new MemoryTrackingEagerBatchingIterator(iterator, size, memoryTracker)
    }
  }

  implicit class CypherRowClosingIterator[T <: CypherRow](val iterator: ClosingIterator[T]) {

    /** Convenience method for adding a variable to all rows */
    def withVariable(key: String, value: AnyValue): ClosingIterator[T] = iterator.map { row =>
      row.set(key, value)
      row
    }

    /** Convenience method for adding a variable to all rows */
    def withVariable(slotOffset: Int, value: AnyValue): ClosingIterator[T] = iterator.map { row =>
      row.setRefAt(slotOffset, value)
      row
    }
  }

  /**
   * An empty closing iterator.
   * This cannot be a val, since resources can be mutated.
   */
  def empty: ClosingIterator[Nothing] = new ClosingIterator[Nothing] {
    override protected[this] def innerHasNext: Boolean = false
    override def next(): Nothing = throw new NoSuchElementException("next on empty iterator")
    override protected[this] def closeMore(): Unit = ()
  }

  /**
   * A closing iterator that returns exactly one element.
   */
  def single[T](elem: T): ClosingIterator[T] = new ClosingIterator[T] {
    private var _hasNext = true

    override protected[this] def closeMore(): Unit = ()

    override protected[this] def innerHasNext: Boolean = _hasNext

    override def next(): T = {
      if (_hasNext) {
        _hasNext = false
        elem
      } else {
        ClosingIterator.empty.next()
      }
    }
  }

  /**
   * Wrap a normal iterator in a closing iterator.
   */
  def apply[T](iterator: Iterator[T]): ClosingIterator[T] = new DelegatingClosingIterator(iterator)
  def apply[T](iterator: java.util.Iterator[T]): ClosingIterator[T] = new DelegatingClosingJavaIterator(iterator)
  def apply[T](elems: T*): ClosingIterator[T] = new DelegatingClosingIterator(elems.iterator)

  def asClosingIterator[T](seq: GenTraversableOnce[T]): ClosingIterator[T] =
    new DelegatingClosingIterator(seq.toIterator)

  def asClosingIterator[T](iterator: java.util.Iterator[T]): ClosingIterator[T] =
    new DelegatingClosingJavaIterator(iterator)

  class DelegatingClosingIterator[+T](iterator: Iterator[T]) extends ClosingIterator[T] {
    override protected[this] def closeMore(): Unit = ()

    override protected[this] def innerHasNext: Boolean = iterator.hasNext

    override def next(): T = iterator.next()
  }

  class DelegatingClosingJavaIterator[+T](iterator: java.util.Iterator[T]) extends ClosingIterator[T] {
    override protected[this] def closeMore(): Unit = ()

    override protected[this] def innerHasNext: Boolean = iterator.hasNext

    override def next(): T = iterator.next()
  }

  /**
   * ClosingIterator that groups its source in eager batches of the specified size.
   *
   * @param source source data
   * @param batchSize size of eager batches
   * @param memoryTracker memory tracker
   * @tparam T type of data
   */
  class MemoryTrackingEagerBatchingIterator[T <: Measurable](
    source: ClosingIterator[T],
    batchSize: Long,
    memoryTracker: MemoryTracker
  ) extends ClosingIterator[EagerBuffer[T]] {
    require(batchSize >= 1, s"Batch size $batchSize smaller than 1")
    memoryTracker.allocateHeap(MemoryTrackingEagerBatchingIterator.SHALLOW_SIZE)

    override protected[this] def closeMore(): Unit = {
      source.close()
      memoryTracker.releaseHeap(MemoryTrackingEagerBatchingIterator.SHALLOW_SIZE)
    }

    override protected[this] def innerHasNext: Boolean = source.hasNext

    /**
     * Returns the next eagerly buffered items, caller is responsible to close the [[EagerBuffer]].
     */
    override def next(): EagerBuffer[T] = {
      val buffer = createEagerBuffer[T](memoryTracker, math.min(batchSize, INIT_CHUNK_SIZE).toInt)
      var count: Long = 0
      while (count < batchSize && source.hasNext) {
        buffer.add(source.next())
        count += 1
      }
      if (count == 0) {
        throw new NoSuchElementException("next on empty iterator")
      }
      buffer
    }
  }

  object MemoryTrackingEagerBatchingIterator {
    final private val SHALLOW_SIZE = shallowSizeOfInstance(classOf[MemoryTrackingEagerBatchingIterator[_]])
    final private val INIT_CHUNK_SIZE = 1024

  }
}

/**
 * Adds the method [[close]] over the normal [[LongIterator]] interface.
 * Always calls close when exhausted automatically.
 */
abstract class ClosingLongIterator extends LongIterator {
  def close(): Unit

  /**
   * Implements the test of whether this iterator can provide another element.
   */
  @inline
  protected[this] def innerHasNext: Boolean

  final override def hasNext: Boolean = {
    val _hasNext = innerHasNext
    if (!_hasNext) {
      close()
    }
    _hasNext
  }
}

object ClosingLongIterator {

  def empty: ClosingLongIterator = new ClosingLongIterator {
    override def close(): Unit = {}
    override protected[this] def innerHasNext: Boolean = false
    override def next(): Long = throw new NoSuchElementException("next on empty iterator")
  }

  def emptyClosingRelationshipIterator: ClosingLongIterator with RelationshipIterator =
    new ClosingLongIterator with RelationshipIterator {
      override def close(): Unit = ()

      override protected[this] def innerHasNext: Boolean = false

      override def relationshipVisit[EXCEPTION <: Exception](
        relationshipId: Long,
        visitor: RelationshipVisitor[EXCEPTION]
      ): Boolean = false

      override def next(): Long = ClosingIterator.empty.next()

      override def startNodeId(): Long = fail()

      override def endNodeId(): Long = fail()

      override def typeId(): Int = fail()

      private def fail() = throw new IllegalStateException("Iterator is empty")
    }

  def prepend(valueToPrepend: Long, iterator: ClosingLongIterator): ClosingLongIterator = new ClosingLongIterator {
    private var first = true

    override def close(): Unit = {
      iterator.close()
    }

    override protected[this] def innerHasNext: Boolean = if (first) true else iterator.hasNext

    override def next(): Long =
      if (first) {
        first = false
        valueToPrepend
      } else {
        iterator.next()
      }
  }
}
