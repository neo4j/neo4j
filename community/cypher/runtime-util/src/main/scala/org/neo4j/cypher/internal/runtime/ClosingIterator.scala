/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.io.IOUtils
import org.neo4j.storageengine.api.RelationshipVisitor

import scala.collection.GenTraversableOnce
import scala.collection.Iterator
import scala.collection.Iterator.empty
import scala.collection.immutable
import scala.collection.mutable
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
        case _ =>
      }
    }
  }

  def withFilter(p: T => Boolean): ClosingIterator[T] = filter(p)

  // except for closing a copy of [[Iterator.filter]]
  def filter(p: T => Boolean): ClosingIterator[T] = new ClosingIterator[T] {
    private var hd: T = _
    private var hdDefined: Boolean = false

    override protected[this]def innerHasNext: Boolean = hdDefined || {
      do {
        if (!self.hasNext) return false
        hd = self.next()
      } while (!p(hd))
      hdDefined = true
      true
    }

    def next(): T = if (hasNext) { hdDefined = false; hd } else empty.next()

    override protected[this] def closeMore(): Unit = self.close()
  }

  // except for closing a copy of [[Iterator.map]]
  def map[B](f: T => B): ClosingIterator[B] = new ClosingIterator[B] {
    override protected[this] def innerHasNext: Boolean = self.hasNext

    def next(): B = f(self.next())

    override protected[this] def closeMore(): Unit = self.close()
  }

  // We can't override Iterator.grouped since that is only accepting Int and not Long.
  // This is our own implementation, [[Iterator.grouped]] supports more use cases that we don't need.
  def grouped(batchSize: Long): ClosingIterator[Seq[T]] = {
    if (batchSize < 1) throw new IllegalArgumentException("Group size should be 1 or larger")

    new ClosingIterator[Seq[T]] {
      override protected[this] def innerHasNext: Boolean = self.hasNext

      def next(): Seq[T] = {
        var counter = 0
        val buffer = mutable.ArrayBuffer[T]()
        while (counter < batchSize && self.hasNext) {
          buffer += self.next()
          counter += 1
        }
        buffer
      }.toSeq

      override protected[this] def closeMore(): Unit = self.close()
    }
  }

  // this is our own implementation, [[Iterator.++]] is overly complex, we probably don't need to be so specialized.
  def ++[B >: T](that: => ClosingIterator[B]): ClosingIterator[B] = new ClosingIterator[B] {
    // We read this into a lazy local variable here to avoid creating a new `that` iterator multiple times.
    // This is OK, since we expect to close both sides anyway.
    private lazy val eagerThat = that

    private var cur: ClosingIterator[B] = self

    override protected[this] def innerHasNext: Boolean = {
      if (cur.hasNext) {
        true
      } else if (cur eq self) {
        cur = eagerThat
        cur.hasNext
      } else {
        false
      }
    }

    def next(): B = if (hasNext) cur.next() else ClosingIterator.empty.next()

    override protected[this] def closeMore(): Unit = {
      self.close()
      eagerThat match {
        case closingIterator: ClosingIterator[_] => closingIterator.close()
        case _ =>
      }
    }
  }

  //NOTE: direct port of Iterator#collect except for closing the inner iterator
  def collect[B](pf: PartialFunction[T, B]): ClosingIterator[B] = new ClosingIterator[B] {
    // Manually buffer to avoid extra layer of wrapping with buffered
    private[this] var hd: T = _

    // Little state machine to keep track of where we are
    // Seek = 0; Found = 1; Empty = -1
    // Not in vals because scalac won't make them static (@inline def only works with -optimize)
    // BE REALLY CAREFUL TO KEEP COMMENTS AND NUMBERS IN SYNC!
    private[this] var status = 0/*Seek*/

    protected override def innerHasNext: Boolean = {
      while (status == 0/*Seek*/) {
        if (self.hasNext) {
          hd = self.next()
          if (pf.isDefinedAt(hd)) status = 1/*Found*/
        }
        else status = -1/*Empty*/
      }
      status == 1/*Found*/
    }
    def next(): B = if (hasNext) { status = 0/*Seek*/; pf(hd) } else ClosingIterator.empty.next()

    override protected[this] def closeMore(): Unit = {
      self.close()
    }
  }
}



object ClosingIterator {

  implicit class JavaIteratorAsClosingIterator[T](val iterator: java.util.Iterator[T]) {
    def asClosingIterator: ClosingIterator[T] = new DelegatingClosingJavaIterator(iterator)
  }

  implicit class JavaCollectionAsClosingIterator[T](val collection: java.util.Collection[T]) {
    def asClosingIterator: ClosingIterator[T] = new DelegatingClosingJavaIterator(collection.iterator())
  }

  implicit class ScalaSeqAsClosingIterator[T](val seq: GenTraversableOnce[T]) {
    def asClosingIterator: ClosingIterator[T] =new DelegatingClosingIterator(seq.toIterator)
  }

  implicit class OptionAsClosingIterator[T](val option: Option[T]) {
    def asClosingIterator: ClosingIterator[T] =new DelegatingClosingIterator(option.toIterator)
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

  def asClosingIterator[T](seq: GenTraversableOnce[T]): ClosingIterator[T] = new DelegatingClosingIterator(seq.toIterator)
  def asClosingIterator[T](iterator: java.util.Iterator[T]): ClosingIterator[T] = new DelegatingClosingJavaIterator(iterator)

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

  override final def hasNext: Boolean = {
    val _hasNext = innerHasNext
    if (!_hasNext) {
      close()
    }
    _hasNext
  }
}

object ClosingLongIterator {
  def emptyClosingRelationshipIterator: ClosingLongIterator with RelationshipIterator = new ClosingLongIterator with RelationshipIterator {
    override def close(): Unit = ()

    override protected[this] def innerHasNext: Boolean = false

    override def relationshipVisit[EXCEPTION <: Exception](relationshipId: Long,
                                                           visitor: RelationshipVisitor[EXCEPTION]): Boolean = false

    override def next(): Long = ClosingIterator.empty.next()

    override def startNodeId(): Long = ???

    override def endNodeId(): Long = ???

    override def typeId(): Int = ???
  }
}
