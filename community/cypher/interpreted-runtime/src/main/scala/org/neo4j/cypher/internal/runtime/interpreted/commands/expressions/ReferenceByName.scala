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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.eclipse.collections.impl.factory.primitive.IntSets
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ValuePopulation
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyPropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue

case class ReferenceByName(col: String) extends Expression {

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: collection.Seq[Expression] = Seq.empty

  override def children: collection.Seq[AstNode[_]] = Seq.empty

  override def apply(row: ReadableRow, state: QueryState): AnyValue = row.getByName(col)
}

case class ValuePopulatingReferenceByName(col: String, cachedProperties: Array[(LazyPropertyKey, Expression)])
    extends Expression {

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: collection.Seq[Expression] = Seq.empty

  override def children: collection.Seq[AstNode[_]] = Seq.empty

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val result = row.getByName(col)
    if (state.prePopulateResults) {
      result match {
        case node: VirtualNodeValue        => populate(node, row, state)
        case rel: VirtualRelationshipValue => populate(rel, row, state)
        case v                             => v
      }
    } else {
      result
    }
  }

  private def populate(node: VirtualNodeValue, row: ReadableRow, state: QueryState): VirtualNodeValue = {
    node match {
      case n: NodeValue => n
      case _ =>
        val cachedTokens = IntSets.mutable.empty()
        val builder = new MapValueBuilder()
        cachedProperties.foreach {
          case (p, e) =>
            cachedTokens.add(p.id(state.query))
            val value = e(row, state)
            builder.add(p.name, value)
        }
        ValuePopulation.nodeValue(
          node.id(),
          state.query,
          state.cursors.nodeCursor,
          state.cursors.propertyCursor,
          builder,
          cachedTokens
        )
    }
  }

  private def populate(rel: VirtualRelationshipValue, row: ReadableRow, state: QueryState): VirtualRelationshipValue = {
    rel match {
      case n: RelationshipValue => n
      case _ =>
        val cachedTokens = IntSets.mutable.empty()
        val builder = new MapValueBuilder()
        cachedProperties.foreach {
          case (p, e) =>
            cachedTokens.add(p.id(state.query))
            val value = e(row, state)
            builder.add(p.name, value)
        }
        ValuePopulation.relationshipValue(
          rel.id(),
          state.query,
          state.cursors.relationshipScanCursor,
          state.cursors.propertyCursor,
          builder,
          cachedTokens
        )
    }
  }
}
