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

import org.eclipse.collections.impl.factory.primitive.IntSets
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ValuePopulation.MISSING_NODE
import org.neo4j.cypher.internal.runtime.ValuePopulation.properties
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyPropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.EMPTY_STRING
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

case class RelationshipFromSlot(offset: Int) extends Expression with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): VirtualRelationshipValue =
    state.query.relationshipById(row.getLongAt(offset))

  override def children: Seq[AstNode[_]] = Seq.empty
}

case class ValuePopulatingRelationshipFromSlot(offset: Int, cachedProperties: Array[(LazyPropertyKey, Expression)])
    extends Expression
    with SlottedExpression {

  override def apply(row: ReadableRow, state: QueryState): VirtualRelationshipValue = {
    if (state.prePopulateResults) {
      val dbAccess = state.query
      val id = row.getLongAt(offset)
      val relCursor = state.cursors.relationshipScanCursor
      dbAccess.singleRelationship(id, relCursor)
      val idMapper = dbAccess.elementIdMapper
      val elementId = idMapper.relationshipElementId(id)

      if (!relCursor.next()) {
        // the relationship has probably been deleted, we still return it but just a bare id
        VirtualValues.relationshipValue(
          id,
          elementId,
          MISSING_NODE,
          MISSING_NODE,
          EMPTY_STRING,
          EMPTY_MAP,
          true
        )
      } else {
        val cachedTokens = IntSets.mutable.empty()
        val builder = new MapValueBuilder()
        cachedProperties.foreach {
          case (p, e) =>
            cachedTokens.add(p.id(dbAccess))
            val value = e(row, state)
            if (value ne Values.NO_VALUE) {
              builder.add(p.name, value)
            }
        }
        val start = VirtualValues.node(relCursor.sourceNodeReference, idMapper)
        val end = VirtualValues.node(relCursor.targetNodeReference, idMapper)
        val propertyCursor = state.cursors.propertyCursor
        relCursor.properties(propertyCursor, ALL_PROPERTIES.excluding((value: Int) => cachedTokens.contains(value)))
        VirtualValues.relationshipValue(
          id,
          elementId,
          start,
          end,
          Values.stringValue(dbAccess.relationshipTypeName(relCursor.`type`)),
          properties(propertyCursor, dbAccess, builder)
        )
      }
    } else {
      state.query.relationshipById(row.getLongAt(offset))
    }
  }

  override def children: Seq[AstNode[_]] = Seq.empty
}
