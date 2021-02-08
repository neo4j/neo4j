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

import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue

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

  class NodeIndexIterator(state: QueryState,
                          queryContext: QueryContext,
                          baseContext: CypherRow,
                          cursor: NodeValueIndexCursor
                         ) extends IndexIteratorBase[CypherRow](state, cursor) {

    override protected def fetchNext(): CypherRow = {
      if (cursor.next()) {
        val newContext = rowFactory.copyWith(baseContext, ident, queryContext.nodeById(cursor.nodeReference()))
        var i = 0
        while (i < indexPropertyIndices.length) {
          newContext.setCachedProperty(indexCachedProperties(i).runtimeKey, cursor.propertyValue(indexPropertyIndices(i)))
          i += 1
        }
        newContext
      } else null
    }
  }

  class RelIndexIterator(state: QueryState,
                         startNode: String,
                         endNode: String,
                         queryContext: QueryContext,
                         baseContext: CypherRow,
                         cursor: RelationshipValueIndexCursor
                        ) extends IndexIteratorBase[CypherRow](state, cursor) {

    override protected def fetchNext(): CypherRow = {
      if (cursor.next()) {

        val relationship = state.query.relationshipById(cursor.relationshipReference())
        val newContext = rowFactory.copyWith(baseContext, ident, relationship, startNode, relationship.startNode(), endNode, relationship.endNode())
        var i = 0
        while (i < indexPropertyIndices.length) {
          newContext.setCachedProperty(indexCachedProperties(i).runtimeKey, cursor.propertyValue(indexPropertyIndices(i)))
          i += 1
        }
        newContext
      } else {
        null
      }
    }
  }

  class UndirectedRelIndexIterator(state: QueryState,
                                   startNode: String,
                                   endNode: String,
                                   queryContext: QueryContext,
                                   baseContext: CypherRow,
                                   cursor: RelationshipValueIndexCursor) extends IndexIteratorBase[CypherRow](state, cursor) {

    //base class is falling fetchNext, meaning that `fetchNext` is called before this constructor runs
    private var emitSibling: Boolean = if (emitSibling) true else false
    private var lastRelationship: RelationshipValue = _
    private var lastStart: NodeValue = _
    private var lastEnd: NodeValue = _

    override protected def fetchNext(): CypherRow = {
      val newContext = if (emitSibling) {
        emitSibling = false
        rowFactory.copyWith(baseContext, ident, lastRelationship, startNode, lastEnd, endNode, lastStart)
      } else if (cursor.next()) {
        emitSibling = true
        lastRelationship = queryContext.relationshipById(cursor.relationshipReference())
        lastStart = lastRelationship.startNode()
        lastEnd = lastRelationship.endNode()
        rowFactory.copyWith(baseContext, ident, lastRelationship, startNode, lastStart, endNode, lastEnd)
      } else {
        null
      }

      if (newContext != null) {
        var i = 0
        while (i < indexPropertyIndices.length) {
          newContext.setCachedProperty(indexCachedProperties(i).runtimeKey, cursor.propertyValue(indexPropertyIndices(i)))
          i += 1
        }
      }
      newContext
    }
  }

}
