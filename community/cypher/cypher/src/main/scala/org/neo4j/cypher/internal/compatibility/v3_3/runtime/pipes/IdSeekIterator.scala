/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.spi.v3_3.Operations
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.PropertyContainer
import org.neo4j.graphdb.Relationship
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValues

abstract class IdSeekIterator[T <: PropertyContainer] extends Iterator[ExecutionContext] with NumericHelper {

  private var cachedEntity: T = computeNextEntity()

  protected def operations: Operations[T]
  protected def entityIds: Iterator[AnyValue]
  protected def asAnyValue(entity: T): AnyValue

  protected def hasNextEntity = cachedEntity != null

  protected def nextEntity(): T = {
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
      val id = asLongEntityId(entityIds.next())
      if (operations.exists(id.longValue()))
        return operations.getById(id.longValue())

    }
    null.asInstanceOf[T]
  }
}

final class NodeIdSeekIterator(ident: String,
                               baseContext: ExecutionContext,
                               protected val operations: Operations[Node],
                               protected val entityIds: Iterator[AnyValue])
    extends IdSeekIterator[Node] {

  def hasNext: Boolean = hasNextEntity

  def next(): ExecutionContext =
    baseContext.newWith1(ident, asAnyValue(nextEntity()))

  override protected def asAnyValue(entity: Node): AnyValue = AnyValues.asNodeValue(entity)
}

final class DirectedRelationshipIdSeekIterator(ident: String,
                                               fromNode: String,
                                               toNode: String,
                                               baseContext: ExecutionContext,
                                               protected val operations: Operations[Relationship],
                                               protected val entityIds: Iterator[AnyValue])
    extends IdSeekIterator[Relationship] {

  def hasNext: Boolean = hasNextEntity

  def next(): ExecutionContext = {
    val rel = nextEntity()
    baseContext.newWith3(ident,
                         AnyValues.asEdgeValue(rel),
                         fromNode,
                         AnyValues.asNodeValue(rel.getStartNode),
                         toNode,
                         AnyValues.asNodeValue(rel.getEndNode))
  }

  override protected def asAnyValue(entity: Relationship): AnyValue = AnyValues.asEdgeValue(entity)
}

final class UndirectedRelationshipIdSeekIterator(ident: String,
                                                 fromNode: String,
                                                 toNode: String,
                                                 baseContext: ExecutionContext,
                                                 protected val operations: Operations[Relationship],
                                                 protected val entityIds: Iterator[AnyValue])
    extends IdSeekIterator[Relationship] {

  private var lastEntity: Relationship = null
  private var lastStart: Node          = null
  private var lastEnd: Node            = null
  private var emitSibling              = false

  def hasNext: Boolean = emitSibling || hasNextEntity

  def next(): ExecutionContext = {
    if (emitSibling) {
      emitSibling = false
      baseContext.newWith3(ident,
                           AnyValues.asEdgeValue(lastEntity),
                           fromNode,
                           AnyValues.asNodeValue(lastEnd),
                           toNode,
                           AnyValues.asNodeValue(lastStart))
    } else {
      emitSibling = true
      lastEntity = nextEntity()
      lastStart = lastEntity.getStartNode
      lastEnd = lastEntity.getEndNode
      baseContext.newWith3(ident,
                           AnyValues.asEdgeValue(lastEntity),
                           fromNode,
                           AnyValues.asNodeValue(lastStart),
                           toNode,
                           AnyValues.asNodeValue(lastEnd))
    }
  }
  override protected def asAnyValue(entity: Relationship): AnyValue = AnyValues.asEdgeValue(entity)
}
