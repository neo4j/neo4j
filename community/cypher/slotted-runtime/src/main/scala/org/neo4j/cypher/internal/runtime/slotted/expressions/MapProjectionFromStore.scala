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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.physicalplanning.ast.PropertyMapEntry
import org.neo4j.cypher.internal.physicalplanning.ast.PropertyProjectionEntry
import org.neo4j.cypher.internal.runtime.PropertyTokensResolver
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CursorUtils.entityGetProperties
import org.neo4j.cypher.operations.CypherFunctions.propertiesGet
import org.neo4j.internal.kernel.api.EntityCursor
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues

abstract class MapProjectionFromStore extends Expression with SlottedExpression {

  final private[this] val (tokens: PropertyTokensResolver, nameAliases: Array[String]) = {
    val (mapKeys, propKeys, propTokens) = entries
      .sortBy(_.mapKey)
      .map(e => (e.mapKey, e.property.name, e.property.token.getOrElse(TokenRead.NO_TOKEN)))
      .unzip3
    (PropertyTokensResolver.property(propKeys.toArray, propTokens.toArray), mapKeys.toArray)
  }

  override def children: Seq[AstNode[_]] = Seq.empty

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    tokens.populate(state.query)

    val id = row.getLongAt(entityOffset)
    if (id == StatementConstants.NO_SUCH_ENTITY) {
      Values.NO_VALUE
    } else {
      val cursor = entityCursor(id, state)
      if (!cursor.next()) {
        Values.NO_VALUE
      } else {
        val values = entityGetProperties(cursor, state.cursors.propertyCursor, tokens.tokens())
        val result = new MapValueBuilder(values.length)
        values.indices.foreach(i => result.add(nameAliases(i), values(i)))
        result.build()
      }
    }
  }

  protected def entityOffset: Int
  protected def entries: Seq[PropertyMapEntry]
  protected def entityCursor(id: Long, state: QueryState): EntityCursor
}

case class NodeProjectionFromStore(entityOffset: Int, entries: Seq[PropertyMapEntry])
    extends MapProjectionFromStore {

  override protected def entityCursor(id: Long, state: QueryState): EntityCursor = {
    val cursor = state.cursors.nodeCursor
    state.query.singleNode(id, cursor)
    cursor
  }
}

case class RelationshipProjectionFromStore(entityOffset: Int, entries: Seq[PropertyMapEntry])
    extends MapProjectionFromStore {

  override protected def entityCursor(id: Long, state: QueryState): EntityCursor = {
    val cursor = state.cursors.relationshipScanCursor
    state.query.singleRelationship(id, cursor)
    cursor
  }
}

case class PropertyProjection(mapExpression: Expression, entries: Seq[PropertyProjectionEntry])
    extends Expression with SlottedExpression {
  private val outerKeys = entries.map(_.key).toArray
  private val innerKeys = entries.map(_.propertyKeyName.name).toArray

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val map = mapExpression(row, state)
    if (map eq Values.NO_VALUE) {
      Values.NO_VALUE
    } else {
      val cursors = state.cursors
      VirtualValues.map(
        outerKeys,
        propertiesGet(
          innerKeys,
          map,
          state.query,
          cursors.nodeCursor,
          cursors.relationshipScanCursor,
          cursors.propertyCursor
        )
      )
    }
  }

  override def children: collection.Seq[AstNode[_]] = Seq.empty
}
