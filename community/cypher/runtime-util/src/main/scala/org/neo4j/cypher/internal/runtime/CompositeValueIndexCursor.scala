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

import java.util.Comparator

import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.ValueComparator
import org.neo4j.values.storable.Values.COMPARATOR

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Cursor used for joining several cursors into one composite cursor.
  */
abstract class CompositeValueIndexCursor extends DefaultCloseListenable with NodeValueIndexCursor {

  private[this] var closed = false
  protected var current: NodeValueIndexCursor = _

  protected def cursors: Array[NodeValueIndexCursor]

  override def numberOfProperties(): Int = current.numberOfProperties()

  override def propertyKey(offset: Int): Int = current.propertyKey(offset)

  override def hasValue: Boolean = current.hasValue

  override def propertyValue(offset: Int): Value = current.propertyValue(offset)

  override def node(cursor: NodeCursor): Unit = current.node(cursor)

  override def nodeReference(): Long = current.nodeReference()

  override def score(): Float = current.score()

  override def setTracer(tracer: KernelReadTracer): Unit = {
    cursors.foreach(_.setTracer(tracer))
  }

  override def removeTracer(): Unit = {
    cursors.foreach(_.removeTracer())
  }

  override def close(): Unit = {
    closed = true
    closeInternal()
    val listener = closeListener
    if (listener != null) listener.onClosed(this)
  }

  override def closeInternal(): Unit = {
    cursors.foreach(_.close())
  }

  override def isClosed: Boolean = closed
}

object CompositeValueIndexCursor {
  private def compare(x: NodeValueIndexCursor, y: NodeValueIndexCursor, comparator: Comparator[Value]): Int = {
    val np = x.numberOfProperties()
    require(y.numberOfProperties() == np)
    var i = 0
    while (i < np) {
      val cmp = comparator.compare(x.propertyValue(i), y.propertyValue(i))
      if (cmp != 0) {
        return cmp
      }
      i += 1
    }
    0
  }

  private val REVERSE_COMPARATOR = new Comparator[Value] {
    override def compare(o1: Value, o2: Value): Int = -COMPARATOR.compare(o1, o2)
  }
  private val ASCENDING: Ordering[NodeValueIndexCursor] =
    (x: NodeValueIndexCursor, y: NodeValueIndexCursor) => compare(x, y, REVERSE_COMPARATOR)

  private val DESCENDING: Ordering[NodeValueIndexCursor] =
    (x: NodeValueIndexCursor, y: NodeValueIndexCursor) => compare(x, y, COMPARATOR)

  def ascending(cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor = new MergeSortCursor(cursors, ASCENDING)
  def descending(cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor = new MergeSortCursor(cursors, DESCENDING)
  def unordered(cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor = new UnorderedCursor(cursors)
}

private class UnorderedCursor(override val cursors: Array[NodeValueIndexCursor]) extends CompositeValueIndexCursor {
  private[this] var index = 0
  current = if (cursors.nonEmpty) cursors.head else null

  @tailrec
  override final def next(): Boolean = {
    if (current != null && current.next()) true
    else {
      if (index < cursors.length - 1) {
        index += 1
        current = cursors(index)
        next()
      } else false
    }
  }
}

/**
  * NOTE: this assumes that cursors internally are correctly sorted which is guaranteed by the index.
  */
private class MergeSortCursor(override val cursors: Array[NodeValueIndexCursor], ordering: Ordering[NodeValueIndexCursor] ) extends CompositeValueIndexCursor {
  private[this] val queue: mutable.PriorityQueue[NodeValueIndexCursor] = mutable.PriorityQueue.empty[NodeValueIndexCursor](ordering)

  cursors.foreach(c => {
    if (c.next()) {
      queue.enqueue(c)
    }
  })

  override def next(): Boolean = {
    if (current != null && current.next()) {
      queue.enqueue(current)
    }

    if (queue.isEmpty) {
      current = null
      false
    }
    else {
      current = queue.dequeue()
      true
    }
  }
}