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

import org.eclipse.collections.api.block.function.primitive.LongToLongFunction
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction
import org.neo4j.storageengine.api.RelationshipVisitor

object PrimitiveLongHelper {

  def map[T](in: ClosingLongIterator, f: LongToObjectFunction[T]): ClosingIterator[T] = new ClosingIterator[T] {
    override def innerHasNext: Boolean = in.hasNext

    override def next(): T = f.apply(in.next())

    override protected[this] def closeMore(): Unit = in.close()
  }

  def mapPrimitive(in: ClosingLongIterator, f: LongToLongFunction): ClosingLongIterator = new ClosingLongIterator {
    override def innerHasNext: Boolean = in.hasNext

    override def next(): Long = f.applyAsLong(in.next())

    override def close(): Unit = in.close()
  }

  def iteratorFrom(values: Long*): ClosingLongIterator = new ClosingLongIterator {
    private var position = 0
    override def close(): Unit = {}
    override protected[this] def innerHasNext: Boolean = position < values.length

    override def next(): Long = {
      if (!hasNext) {
        throw new NoSuchElementException("next on exhausted iterator")
      }
      val current = values(position)
      position += 1
      current
    }
  }

  def relationshipIteratorFrom(values: (Long, Int, Long, Long)*): ClosingLongIterator with RelationshipIterator =
    new ClosingLongIterator with RelationshipIterator {
      private var position = 0
      override def close(): Unit = {}
      override protected[this] def innerHasNext: Boolean = position < values.length

      override def next(): Long = {
        if (!hasNext) {
          throw new NoSuchElementException("next on exhausted iterator")
        }
        val (current, _, _, _) = values(position)
        position += 1
        current
      }

      override def relationshipVisit[EXCEPTION <: Exception](
        relationshipId: Long,
        visitor: RelationshipVisitor[EXCEPTION]
      ): Boolean = {
        visitor.visit(relationshipId, typeId(), startNodeId(), endNodeId())
        true
      }

      override def startNodeId(): Long = values(position - 1)._3

      override def endNodeId(): Long = values(position - 1)._4

      override def typeId(): Int = values(position - 1)._2
    }
}
