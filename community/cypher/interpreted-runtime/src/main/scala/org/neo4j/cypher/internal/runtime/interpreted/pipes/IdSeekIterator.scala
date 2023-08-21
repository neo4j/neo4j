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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.storageengine.api.RelationshipVisitor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import java.util.NoSuchElementException

class NodeIdSeekIterator(nodeIds: java.util.Iterator[AnyValue], query: QueryContext) extends ClosingLongIterator {
  private var cachedNode: Long = computeNext()
  override def close(): Unit = {}
  override protected[this] def innerHasNext: Boolean = cachedNode != StatementConstants.NO_SUCH_NODE

  override def next(): Long = {
    if (innerHasNext) {
      val result = cachedNode
      cachedNode = computeNext()
      result
    } else {
      throw new NoSuchElementException
    }
  }

  private def computeNext(): Long = {
    while (nodeIds.hasNext) {
      val value = nodeIds.next()
      if (value != Values.NO_VALUE) {
        val id = NumericHelper.asLongEntityIdPrimitive(value)
        if (query.nodeReadOps.entityExists(id)) {
          return id
        }
      }
    }
    StatementConstants.NO_SUCH_NODE
  }

}

case class RelationshipState(id: Long, startNode: Long, endNode: Long, relType: Int) {
  def flip: RelationshipState = copy(startNode = endNode, endNode = startNode)
}

class DirectedRelationshipIdSeekIterator(
  relIds: java.util.Iterator[AnyValue],
  read: Read,
  cursor: RelationshipScanCursor
) extends ClosingLongIterator with RelationshipIterator {
  protected var cachedRelationship: RelationshipState = _
  protected var _next: Long = StatementConstants.NO_SUCH_RELATIONSHIP
  private var startNode = StatementConstants.NO_SUCH_NODE
  private var endNode = StatementConstants.NO_SUCH_NODE
  private var typ = StatementConstants.NO_SUCH_RELATIONSHIP_TYPE
  override def close(): Unit = { cursor.close() }

  override protected[this] def innerHasNext: Boolean = {
    if (cachedRelationship == null) {
      _next = computeNext()
    }
    _next != StatementConstants.NO_SUCH_RELATIONSHIP
  }

  override def relationshipVisit[EXCEPTION <: Exception](
    relationshipId: Long,
    visitor: RelationshipVisitor[EXCEPTION]
  ): Boolean = {
    visitor.visit(relationshipId, typeId(), startNodeId(), endNodeId())
    true
  }

  override def startNodeId(): Long = startNode

  override def endNodeId(): Long = endNode

  override def typeId(): Int = typ

  override def next(): Long = {
    if (innerHasNext) {
      val result = _next
      storeState()
      _next = computeNext()
      result
    } else {
      throw new NoSuchElementException
    }
  }

  protected def storeState(): Unit = {
    startNode = cachedRelationship.startNode
    endNode = cachedRelationship.endNode
    typ = cachedRelationship.relType
  }

  protected def flipState(): Unit = {
    val oldStart = startNode
    startNode = endNode
    endNode = oldStart
  }

  protected def computeNext(): Long = {
    while (relIds.hasNext) {
      val value = relIds.next()
      if (value != Values.NO_VALUE) {
        val id = NumericHelper.asLongEntityIdPrimitive(value)
        if (id >= 0) {
          read.singleRelationship(id, cursor)
          if (cursor.next()) {
            cachedRelationship = RelationshipState(
              cursor.relationshipReference(),
              cursor.sourceNodeReference(),
              cursor.targetNodeReference(),
              cursor.`type`()
            )
            return cursor.relationshipReference()
          }
        }
      }
    }
    StatementConstants.NO_SUCH_RELATIONSHIP
  }
}

class UndirectedRelationshipIdSeekIterator(
  relIds: java.util.Iterator[AnyValue],
  read: Read,
  cursor: RelationshipScanCursor
) extends DirectedRelationshipIdSeekIterator(relIds, read, cursor) {
  private var emitSibling = false
  private var lastRel = -1L

  override protected[this] def innerHasNext: Boolean = emitSibling || super.innerHasNext

  override def next(): Long = {
    if (innerHasNext) {
      if (emitSibling) {
        emitSibling = false
        flipState()
        lastRel
      } else {
        lastRel = _next
        storeState()
        // For self-loops, we don't emit sibling
        emitSibling = startNodeId() != endNodeId()
        _next = computeNext()
        lastRel
      }
    } else {
      throw new NoSuchElementException
    }
  }
}
