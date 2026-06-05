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
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

/**
 * Provides a helper method for index pipes that get nodes together with actual property values.
 */
trait IndexPipeWithValues extends Pipe {

  // Name of the entity variable
  val ident: Option[String]
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

    private val newRow: NodeValueIndexCursor => CypherRow = ident match {
      case Some(node) => (cursor: NodeValueIndexCursor) =>
          rowFactory.copyWith(baseContext, node, queryContext.nodeById(cursor.nodeReference()))
      case None => (_: NodeValueIndexCursor) => baseContext
    }

    override protected def fetchNext(): CypherRow = {
      if (cursor.next()) {
        val newContext = newRow(cursor)
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
    startNode: Option[String],
    endNode: Option[String],
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ) extends IndexIteratorBase[CypherRow](cursor) {
    private val relationshipWriter = Relationships.compileRelationshipWriter(ident, startNode, endNode)

    override protected def fetchNext(): CypherRow = {
      while (cursor.next() && cursor.readFromStore()) {
        val newContext =
          relationshipWriter.writeRow(
            rowFactory,
            baseContext,
            cursor
          )
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
      null
    }
  }

  private class NonStoreAccessingRelIndexIterator(
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ) extends IndexIteratorBase[CypherRow](cursor) {
    private val relationshipWriter = Relationships.compileRelationshipWriter(ident, None, None)

    override protected def fetchNext(): CypherRow = {
      if (cursor.next()) {
        val newContext =
          relationshipWriter.writeRow(
            rowFactory,
            baseContext,
            cursor
          )
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

  object RelIndexIterator {

    def apply(
      startNode: Option[String],
      endNode: Option[String],
      baseContext: CypherRow,
      cursor: RelationshipValueIndexCursor
    ): IndexIteratorBase[CypherRow] = {
      if (startNode.isEmpty && endNode.isEmpty) {
        new NonStoreAccessingRelIndexIterator(baseContext, cursor)
      } else {
        new RelIndexIterator(startNode, endNode, baseContext, cursor)
      }
    }
  }

  class UndirectedRelIndexIterator(
    startNode: Option[String],
    endNode: Option[String],
    baseContext: CypherRow,
    cursor: RelationshipValueIndexCursor
  ) extends IndexIteratorBase[CypherRow](cursor) {

    private val relationshipWriter = Relationships.compileRelationshipWriter(ident, startNode, endNode)

    private var emitSibling: Boolean = false
    private var lastRelationship: VirtualRelationshipValue = _
    private var lastStart: VirtualNodeValue = _
    private var lastEnd: VirtualNodeValue = _

    override protected def fetchNext(): CypherRow = {
      val newContext =
        if (emitSibling) {
          emitSibling = false
          relationshipWriter.writeRow(
            rowFactory,
            baseContext,
            lastRelationship,
            lastEnd,
            lastStart
          )
        } else {
          var ctx: CypherRow = null
          while (ctx == null && cursor.next()) {
            if (cursor.readFromStore()) {
              lastRelationship = ValueUtils.fromRelationshipCursor(cursor)
              val start = cursor.sourceNodeReference()
              val end = cursor.targetNodeReference()
              lastStart = VirtualValues.node(start)
              lastEnd = VirtualValues.node(end)
              // For self-loops, we don't emit sibling
              emitSibling = start != end
              ctx = relationshipWriter.writeRow(
                rowFactory,
                baseContext,
                lastRelationship,
                lastStart,
                lastEnd
              )

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
