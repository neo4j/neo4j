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
package org.neo4j.cypher.internal.physicalplanning

import scala.reflect.ClassTag

/**
  * Wrapper for Array that only exposes read functionality.
  *
  * This was needed to
  *   1) micro-optimize the pipelined runtime code where scala collections were too slow. This slowness
  *      comes from having general solutions and mega-morphic call sites preventing inlining.
  *   2) prevent accidental modification of the arrays, which could happen if we used standard arrays.
  */
class ReadOnlyArray[T](private val inner: Array[T]) {

  final val length = inner.length
  final val isEmpty = inner.length == 0
  final def nonEmpty: Boolean = !isEmpty

  def apply(i: Int): T = inner(i)

  def filter(predicate: T => Boolean): ReadOnlyArray[T] = {
    new ReadOnlyArray[T](inner.filter(predicate))
  }

  def map[U](f: T => U): ReadOnlyArray[U] = {
    val result = new Array[Any](length)
    var i = 0
    while (i < length) {
      result(i) = f(inner(i))
      i += 1
    }
    new ReadOnlyArray[U](result.asInstanceOf[Array[U]])
  }

  def foreach(f: T => Unit): Unit = {
    var i = 0
    while (i < length) {
      f(inner(i))
      i += 1
    }
  }

  /**
    * Return this data in a `Seq`. Not for hot path use.
    */
  def toSeq: Seq[T] = inner.toSeq

  /**
   * Return a copy of the inner array.
   */
  def toArray[U >: T : ClassTag]: Array[U] = {
    val result = new Array[U](length)
    System.arraycopy(inner, 0, result, 0, length)
    result
  }

  /**
    * Return a copy of this array with an appended element `t`. Not for hot path use.
    */
  def :+(t: T): ReadOnlyArray[T] = {
    val result = new Array[Any](length + 1)
    System.arraycopy(inner, 0, result, 0, length)
    result(length) = t
    new ReadOnlyArray[T](result.asInstanceOf[Array[T]])
  }

  /**
    * Return a copy of this array with a prepended element `t`. Not for hot path use.
    */
  def +:(t: T): ReadOnlyArray[T] = {
    val result = new Array[Any](length + 1)
    System.arraycopy(inner, 0, result, 1, length)
    result(0) = t
    new ReadOnlyArray[T](result.asInstanceOf[Array[T]])
  }

  // Equals and hashCode are implemented to simplify testing

  def canEqual(other: Any): Boolean = other.isInstanceOf[ReadOnlyArray[_]]

  override def toString: String = inner.mkString("[", ", ", "]")

  override def equals(other: Any): Boolean =
    other match {
      case that: ReadOnlyArray[_] =>
        (that canEqual this) && inner.toSeq == that.toSeq
      case _ => false
    }

  override def hashCode(): Int = {
    inner.foldLeft(0)((a, b) => 31 * a + b.hashCode())
  }
}

object ReadOnlyArray {
  def empty[T: ClassTag]: ReadOnlyArray[T] = new ReadOnlyArray[T](Array.empty[T])

  def apply[T: ClassTag](ts: T*): ReadOnlyArray[T] = new ReadOnlyArray[T](ts.toArray)
}
