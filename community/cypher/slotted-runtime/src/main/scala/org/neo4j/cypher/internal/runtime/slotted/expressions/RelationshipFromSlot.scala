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
import org.neo4j.cypher.internal.runtime.ValuePopulation
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyPropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualRelationshipValue

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
      val query = state.query
      val id = row.getLongAt(offset)
      val cachedTokens = IntSets.mutable.empty()
      val builder = new MapValueBuilder()
      cachedProperties.foreach {
        case (p, e) =>
          cachedTokens.add(p.id(query))
          val value = e(row, state)
          builder.add(p.name, value)
      }
      ValuePopulation.relationshipValue(
        id,
        query,
        state.cursors.relationshipScanCursor,
        state.cursors.propertyCursor,
        builder,
        cachedTokens
      )
    } else {
      state.query.relationshipById(row.getLongAt(offset))
    }
  }

  override def children: Seq[AstNode[_]] = Seq.empty
}
