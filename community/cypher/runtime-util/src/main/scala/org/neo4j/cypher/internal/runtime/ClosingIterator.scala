/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

/**
 * Adds the method [[close]] over the normal [[Iterator]] interface.
 * Always calls close when exhausted automatically.
 * Allows to register resources to get closed as well.
 *
 * Overrides scala [[Iterator]] convenience functions to create new
 * [[ClosingIterator]]s that propagate [[close]] calls.
 */
abstract class ClosingIterator[+T] extends Iterator[T] with AutoCloseable {
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

  /**
   * Implements the test of whether this iterator can provide another element.
   */
  @inline
  protected[this] def innerHasNext: Boolean

  // PUBLIC API

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

  override final def hasNext: Boolean = {
    val _hasNext = innerHasNext
    if (!_hasNext) {
      close()
    }
    _hasNext
  }

  // CONVENIENCE METHODS

  // except for closing a copy of [[Iterator.flatMap]]
  override def flatMap[B](f: T => GenTraversableOnce[B]): ClosingIterator[B] = new ClosingIterator[B] {
    private var cur: Iterator[B] = ClosingIterator.empty

    private def nextCur(): Unit = {
      cur = f(self.next()).toIterator
    }

    def innerHasNext: Boolean = {
      while (!cur.hasNext) {
        if (!self.hasNext) return false
        nextCur()
      }
      true
    }

    def next(): B = (if (hasNext) cur else ClosingIterator.empty).next()

    override def closeMore(): Unit = {
      self.close()
      cur match {
        case closingIterator: ClosingIterator[_] => closingIterator.close()
        case _ =>
      }
    }
  }

  override def withFilter(p: T => Boolean): ClosingIterator[T] = filter(p)

  // except for closing a copy of [[Iterator.filter]]
  override def filter(p: T => Boolean): ClosingIterator[T] = new ClosingIterator[T] {
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
  override def map[B](f: T => B): ClosingIterator[B] = new ClosingIterator[B] {
    override protected[this] def innerHasNext: Boolean = self.hasNext

    def next(): B = f(self.next())

    override protected[this] def closeMore(): Unit = self.close()
  }

  // this is our own implementation, [[Iterator.++]] is overly complex, we probably don't need to be so specialized.
  override def ++[B >: T](that: => GenTraversableOnce[B]): ClosingIterator[B] = new ClosingIterator[B] {
    // We cannot change the call-by-name signature if we want to override Iterator.++
    // We read this into a lazy local variable here to avoid creating a new `that` iterator multiple times.
    // This is OK, since we expect to close both sides anyway.
    private lazy val eagerThat = that

    private var cur: Iterator[B] = self

    override protected[this] def innerHasNext: Boolean = {
      if (cur.hasNext) {
        true
      } else if (cur eq self) {
        cur = eagerThat.toIterator
        cur.hasNext
      } else {
        false
      }
    }

    override def next(): B = if (hasNext) cur.next() else ClosingIterator.empty.next()

    override protected[this] def closeMore(): Unit = {
      self.close()
      eagerThat match {
        case closingIterator: ClosingIterator[_] => closingIterator.close()
        case _ =>
      }
    }
  }
}

object ClosingIterator {
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
  def apply[T](iterator: Iterator[T]): ClosingIterator[T] = iterator match {
    case c: ClosingIterator[T] => c
    case _ => new DelegatingClosingIterator(iterator)
  }

  class DelegatingClosingIterator[+T](iterator: Iterator[T]) extends ClosingIterator[T] {
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
  }
}
