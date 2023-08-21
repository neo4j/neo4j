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

import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

/**
 * Provides a helper method for index pipes that get nodes together with actual property values.
 */
trait IndexPipeWithValues extends Pipe {

  // Name of the entity variable
  val ident: String
  // all indices where the index can provide values
  val indexPropertyIndices: Array[Int]
  // the cached properties where we will get values
  val indexCachedProperties: Array[CachedProperty]

  class NodeIndexIterator(
    state: QueryState,
    queryContext: QueryContext,
    baseContext: CypherRow,
    cursor: NodeValueIndexCursor
  ) extends IndexIteratorBase[CypherRow](cursor) {

    override protected def fetchNext(): CypherRow = {
      if (cursor.next()) {
        val newContext = rowFactory.copyWith(baseContext, ident, queryContext.nodeById(cursor.nodeReference()))
        var i = 0
        while (i < indexPropertyIndices.length) {
          newContext.setCachedProperty(
            indexCachedProperties(i).runtimeKey,
            cursor.propertyValue(indexPropertyIndices(i))
          )
          i += 1
        }
        newContext
      } else {
        null
      }
    }
  }

  class RelIndexIterator(
    state: QueryState,
    startNode: String,
    endNode: String,
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ) extends IndexIteratorBase[CypherRow](cursor) {
    private val queryContext = state.query

    override protected def fetchNext(): CypherRow = {
      while (cursor.next()) {

        val relationship = queryContext.relationshipById(cursor.relationshipReference())
        if (cursor.readFromStore()) {
          val source = queryContext.nodeById(cursor.sourceNodeReference())
          val target = queryContext.nodeById(cursor.targetNodeReference())
          val newContext = rowFactory.copyWith(baseContext, ident, relationship, startNode, source, endNode, target)
          var i = 0
          while (i < indexPropertyIndices.length) {
            newContext.setCachedProperty(
              indexCachedProperties(i).runtimeKey,
              cursor.propertyValue(indexPropertyIndices(i))
            )
            i += 1
          }
          return newContext
        }
      }
      null
    }
  }

  class UndirectedRelIndexIterator(
    startNode: String,
    endNode: String,
    state: QueryState,
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ) extends IndexIteratorBase[CypherRow](cursor) {

    private val queryContext = state.query
    private var emitSibling: Boolean = false
    private var lastRelationship: VirtualRelationshipValue = _
    private var lastStart: VirtualNodeValue = _
    private var lastEnd: VirtualNodeValue = _

    override protected def fetchNext(): CypherRow = {
      val newContext =
        if (emitSibling) {
          emitSibling = false
          rowFactory.copyWith(baseContext, ident, lastRelationship, startNode, lastEnd, endNode, lastStart)
        } else {
          var ctx: CypherRow = null
          while (ctx == null && cursor.next()) {
            lastRelationship = queryContext.relationshipById(cursor.relationshipReference())
            if (cursor.readFromStore()) {
              val start = cursor.sourceNodeReference()
              val end = cursor.targetNodeReference()
              lastStart = queryContext.nodeById(start)
              lastEnd = queryContext.nodeById(end)
              // For self-loops, we don't emit sibling
              emitSibling = start != end
              ctx = rowFactory.copyWith(baseContext, ident, lastRelationship, startNode, lastStart, endNode, lastEnd)
            }
          }
          ctx
        }

      if (newContext != null) {
        var i = 0
        while (i < indexPropertyIndices.length) {
          newContext.setCachedProperty(
            indexCachedProperties(i).runtimeKey,
            cursor.propertyValue(indexPropertyIndices(i))
          )
          i += 1
        }
      }
      newContext
    }
  }

}
