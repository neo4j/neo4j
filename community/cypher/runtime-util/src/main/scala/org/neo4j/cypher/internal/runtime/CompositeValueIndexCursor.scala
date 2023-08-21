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

import org.neo4j.internal.kernel.api._
import org.neo4j.io.IOUtils
import org.neo4j.storageengine.api.PropertySelection
import org.neo4j.storageengine.api.Reference
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values.COMPARATOR

import java.util.Comparator

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Cursor used for joining several cursors into one composite cursor.
  */
abstract class CompositeValueIndexCursor[T <: ValueIndexCursor] extends DefaultCloseListenable with ValueIndexCursor {

  protected var closed = false
  protected var current: T = _

  protected def innerNext(t: T): Boolean

  override def numberOfProperties(): Int = current.numberOfProperties()

  override def hasValue: Boolean = current.hasValue

  override def propertyValue(offset: Int): Value = current.propertyValue(offset)

  override def isClosed: Boolean = closed
}

object CompositeValueIndexCursor {

  private def compare(x: ValueIndexCursor, y: ValueIndexCursor, comparator: Comparator[Value]): Int = {
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

  private val ASCENDING: Ordering[ValueIndexCursor] =
    (x: ValueIndexCursor, y: ValueIndexCursor) => compare(x, y, REVERSE_COMPARATOR)

  private val DESCENDING: Ordering[ValueIndexCursor] =
    (x: ValueIndexCursor, y: ValueIndexCursor) => compare(x, y, COMPARATOR)

  def ascending(cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor =
    new MergeSortNodeCursor(cursors, ASCENDING)

  def descending(cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor =
    new MergeSortNodeCursor(cursors, DESCENDING)
  def unordered(cursors: Array[NodeValueIndexCursor]): NodeValueIndexCursor = new UnorderedNodeCursor(cursors)

  def ascending(cursors: Array[RelationshipValueIndexCursor]): RelationshipValueIndexCursor =
    new MergeSortRelationshipCursor(cursors, ASCENDING)

  def descending(cursors: Array[RelationshipValueIndexCursor]): RelationshipValueIndexCursor =
    new MergeSortRelationshipCursor(cursors, DESCENDING)

  def unordered(cursors: Array[RelationshipValueIndexCursor]): RelationshipValueIndexCursor =
    new UnorderedRelationshipCursor(cursors)
}

abstract private class UnorderedCursor[T <: ValueIndexCursor](cursors: Array[T]) extends CompositeValueIndexCursor[T] {
  private[this] var index = 0
  current = if (cursors.nonEmpty) cursors.head else null.asInstanceOf[T]

  @tailrec
  final def next(): Boolean = {
    if (current != null && innerNext(current)) true
    else {
      if (index < cursors.length - 1) {
        index += 1
        current = cursors(index)
        next()
      } else false
    }
  }
}

private class UnorderedNodeCursor(cursors: Array[NodeValueIndexCursor])
    extends UnorderedCursor[NodeValueIndexCursor](cursors) with NodeValueIndexCursor {

  override def setTracer(tracer: KernelReadTracer): Unit = {
    cursors.foreach(_.setTracer(tracer))
  }
  override def score(): Float = current.score()

  override def removeTracer(): Unit = {
    cursors.foreach(_.removeTracer())
  }

  override def closeInternal(): Unit = {
    closed = true
    IOUtils.closeAll(cursors: _*)
  }

  override def node(cursor: NodeCursor): Unit = current.node(cursor)

  override def nodeReference(): Long = current.nodeReference()

  override protected def innerNext(cursor: NodeValueIndexCursor): Boolean = cursor.next()
}

private class UnorderedRelationshipCursor(cursors: Array[RelationshipValueIndexCursor])
    extends UnorderedCursor[RelationshipValueIndexCursor](cursors) with RelationshipValueIndexCursor {

  override def setTracer(tracer: KernelReadTracer): Unit = {
    cursors.foreach(_.setTracer(tracer))
  }
  override def score(): Float = current.score()

  override def removeTracer(): Unit = {
    cursors.foreach(_.removeTracer())
  }

  override def closeInternal(): Unit = {
    closed = true
    IOUtils.closeAll(cursors: _*)
  }

  override protected def innerNext(cursor: RelationshipValueIndexCursor): Boolean = cursor.next()

  override def readFromStore(): Boolean = current.readFromStore()

  override def source(cursor: NodeCursor): Unit = current.source(cursor)

  override def target(cursor: NodeCursor): Unit = current.target(cursor)

  override def `type`(): Int = current.`type`()

  override def sourceNodeReference(): Long = current.sourceNodeReference()

  override def targetNodeReference(): Long = current.targetNodeReference()

  override def relationshipReference(): Long = current.relationshipReference()

  override def properties(cursor: PropertyCursor, selection: PropertySelection): Unit =
    current.properties(cursor, selection)

  override def propertiesReference(): Reference = current.propertiesReference()
}

/**
  * NOTE: this assumes that cursors internally are correctly sorted which is guaranteed by the index.
  */
abstract private class MergeSortCursor[T <: ValueIndexCursor](cursors: Array[T], ordering: Ordering[ValueIndexCursor])
    extends CompositeValueIndexCursor[T] {

  private[this] val queue: mutable.PriorityQueue[ValueIndexCursor] =
    mutable.PriorityQueue.empty[ValueIndexCursor](ordering)

  cursors.foreach(c => {
    if (innerNext(c)) {
      queue.enqueue(c)
    }
  })

  final def next(): Boolean = {
    if (current != null && innerNext(current)) {
      queue.enqueue(current)
    }

    if (queue.isEmpty) {
      current = null.asInstanceOf[T]
      false
    } else {
      current = queue.dequeue().asInstanceOf[T]
      true
    }
  }
}

private class MergeSortNodeCursor(cursors: Array[NodeValueIndexCursor], ordering: Ordering[ValueIndexCursor])
    extends MergeSortCursor[NodeValueIndexCursor](cursors, ordering) with NodeValueIndexCursor {

  override def setTracer(tracer: KernelReadTracer): Unit = {
    cursors.foreach(_.setTracer(tracer))
  }
  override def score(): Float = current.score()

  override def removeTracer(): Unit = {
    cursors.foreach(_.removeTracer())
  }

  override def closeInternal(): Unit = {
    closed = true
    IOUtils.closeAll(cursors: _*)
  }

  override def node(cursor: NodeCursor): Unit = current.node(cursor)
  override def nodeReference(): Long = current.nodeReference()
  override protected def innerNext(cursor: NodeValueIndexCursor): Boolean = cursor.next()
}

private class MergeSortRelationshipCursor(
  cursors: Array[RelationshipValueIndexCursor],
  ordering: Ordering[ValueIndexCursor]
) extends MergeSortCursor[RelationshipValueIndexCursor](cursors, ordering) with RelationshipValueIndexCursor {

  override def setTracer(tracer: KernelReadTracer): Unit = {
    cursors.foreach(_.setTracer(tracer))
  }
  override def score(): Float = current.score()

  override def removeTracer(): Unit = {
    cursors.foreach(_.removeTracer())
  }

  override def closeInternal(): Unit = {
    closed = true
    IOUtils.closeAll(cursors: _*)
  }

  override def readFromStore(): Boolean = current.readFromStore()

  override def source(cursor: NodeCursor): Unit = current.source(cursor)

  override def target(cursor: NodeCursor): Unit = current.target(cursor)

  override def `type`(): Int = current.`type`()

  override def sourceNodeReference(): Long = current.sourceNodeReference()

  override def targetNodeReference(): Long = current.targetNodeReference()

  override def relationshipReference(): Long = current.relationshipReference()

  override protected def innerNext(cursor: RelationshipValueIndexCursor): Boolean = cursor.next()

  override def properties(cursor: PropertyCursor, selection: PropertySelection): Unit =
    current.properties(cursor, selection)

  override def propertiesReference(): Reference = current.propertiesReference()
}
