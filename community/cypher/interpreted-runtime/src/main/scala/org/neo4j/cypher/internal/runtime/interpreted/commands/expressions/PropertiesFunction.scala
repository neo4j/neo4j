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
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyPropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValueBuilder

case class PropertiesFunction(a: Expression) extends Expression {

  override def apply(ctx: ReadableRow, state: QueryState): AnyValue =
    CypherFunctions.properties(
      a(ctx, state),
      state.query,
      state.cursors.nodeCursor,
      state.cursors.relationshipScanCursor,
      state.cursors.propertyCursor
    )

  override def arguments: Seq[Expression] = Seq(a)

  override def children: Seq[AstNode[_]] = Seq(a)

  override def rewrite(f: Expression => Expression): Expression = f(PropertiesFunction(a.rewrite(f)))
}

case class PropertiesUsingCachedExpressionsFunction(
  a: Expression,
  cachedProperties: Array[(LazyPropertyKey, Expression)]
) extends Expression {

  override def apply(ctx: ReadableRow, state: QueryState): AnyValue = {
    val builder = new MapValueBuilder()
    val cachedTokens = IntSets.mutable.empty()
    cachedProperties.foreach {
      case (p, e) =>
        val propertyValue = e(ctx, state)
        if (!(propertyValue eq Values.NO_VALUE)) {
          cachedTokens.add(p.id(state.query))
          builder.add(p.name, propertyValue)
        }
    }

    CypherFunctions.properties(
      a(ctx, state),
      state.query,
      state.cursors.nodeCursor,
      state.cursors.relationshipScanCursor,
      state.cursors.propertyCursor,
      builder,
      cachedTokens
    )
  }

  override def arguments: Seq[Expression] = Seq(a)

  override def children: Seq[AstNode[_]] = Seq(a)

  override def rewrite(f: Expression => Expression): Expression =
    f(PropertiesUsingCachedExpressionsFunction(a.rewrite(f), cachedProperties.map { case (k, v) => k -> v.rewrite(f) }))
}
