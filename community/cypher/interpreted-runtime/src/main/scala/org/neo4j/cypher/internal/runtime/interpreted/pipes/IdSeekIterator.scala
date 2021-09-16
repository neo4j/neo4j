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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
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

class DirectedRelationshipIdSeekIterator(relIds: java.util.Iterator[AnyValue], state: QueryState) extends ClosingLongIterator with RelationshipIterator {
  protected var cachedRelationship: RelationshipState = _
  protected var _next = StatementConstants.NO_SUCH_RELATIONSHIP
  private var startNode = StatementConstants.NO_SUCH_NODE
  private var endNode = StatementConstants.NO_SUCH_NODE
  private var typ = StatementConstants.NO_SUCH_RELATIONSHIP_TYPE
  override def close(): Unit = {}

  override protected[this] def innerHasNext: Boolean = {
    if (cachedRelationship == null) {
      _next = computeNext()
    }
    _next != StatementConstants.NO_SUCH_RELATIONSHIP
  }

  override def relationshipVisit[EXCEPTION <: Exception](relationshipId: Long, visitor: RelationshipVisitor[EXCEPTION]): Boolean = {
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
          val internalCursor = state.cursors.relationshipScanCursor
          state.query.singleRelationship(id, internalCursor)
          if (internalCursor.next()) {
            cachedRelationship = RelationshipState(internalCursor.relationshipReference(),
              internalCursor.sourceNodeReference(),
              internalCursor.targetNodeReference(),
              internalCursor.`type`())
            return internalCursor.relationshipReference()
          }
        }
      }
    }
    StatementConstants.NO_SUCH_RELATIONSHIP
  }
}

class UndirectedRelationshipIdSeekIterator(relIds: java.util.Iterator[AnyValue], state: QueryState) extends DirectedRelationshipIdSeekIterator(relIds, state) {
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
        emitSibling = true
        lastRel = _next
        storeState()
        _next = computeNext()
        lastRel
      }
    } else {
      throw new NoSuchElementException
    }
  }
}
