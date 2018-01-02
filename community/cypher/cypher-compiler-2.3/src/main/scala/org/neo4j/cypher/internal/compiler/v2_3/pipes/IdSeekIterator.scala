/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.compiler.v2_3.spi.Operations
import org.neo4j.cypher.internal.frontend.v2_3.EntityNotFoundException
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

abstract class IdSeekIterator[T <: PropertyContainer]
  extends Iterator[ExecutionContext] with NumericHelper {

  private var cachedEntity: T = computeNextEntity()

  protected def operations: Operations[T]
  protected def entityIds: Iterator[Any]

  protected def hasNextEntity = cachedEntity != null

  protected def nextEntity() = {
    if (hasNextEntity) {
      val result = cachedEntity
      cachedEntity = computeNextEntity()
      result
    } else {
      Iterator.empty.next()
    }
  }

  private def computeNextEntity(): T = {
    while (entityIds.hasNext) {
      try {
        return operations.getById(asLongEntityId(entityIds.next()))
      }
      catch {
        case _: EntityNotFoundException =>
      }
    }
    null.asInstanceOf[T]
  }
}

final class NodeIdSeekIterator(ident: String,
                               baseContext: ExecutionContext,
                               protected val operations: Operations[Node],
                               protected val entityIds: Iterator[Any])
  extends IdSeekIterator[Node] {

  def hasNext: Boolean = hasNextEntity

  def next(): ExecutionContext =
    baseContext.newWith1(ident, nextEntity())
}

final class DirectedRelationshipIdSeekIterator(ident: String,
                                               fromNode: String,
                                               toNode: String,
                                               baseContext: ExecutionContext,
                                               protected val operations: Operations[Relationship],
                                               protected val entityIds: Iterator[Any])
  extends IdSeekIterator[Relationship] {

  def hasNext: Boolean = hasNextEntity

  def next(): ExecutionContext = {
    val rel = nextEntity()
    baseContext.newWith3(ident, rel, fromNode, rel.getStartNode, toNode, rel.getEndNode)
  }
}

final class UndirectedRelationshipIdSeekIterator(ident: String,
                                                 fromNode: String,
                                                 toNode: String,
                                                 baseContext: ExecutionContext,
                                                 protected val operations: Operations[Relationship],
                                                 protected val entityIds: Iterator[Any])
  extends IdSeekIterator[Relationship] {

  private var lastEntity: Relationship = null
  private var lastStart: Node = null
  private var lastEnd: Node = null
  private var emitSibling = false

  def hasNext: Boolean = emitSibling || hasNextEntity

  def next(): ExecutionContext = {
    if (emitSibling) {
      emitSibling = false
      baseContext.newWith3(ident, lastEntity, fromNode, lastEnd, toNode, lastStart)
    } else {
      emitSibling = true
      lastEntity = nextEntity()
      lastStart = lastEntity.getStartNode
      lastEnd = lastEntity.getEndNode
      baseContext.newWith3(ident, lastEntity, fromNode, lastStart, toNode, lastEnd)
    }
  }
}
