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

import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.AnyValue

case class ListSlice(collection: Expression, from: Option[Expression], to: Option[Expression])
    extends Expression with ListSupport {
  override def arguments: Seq[Expression] = from.toIndexedSeq ++ to.toIndexedSeq :+ collection

  override def children: Seq[AstNode[_]] = arguments

  private val function: (AnyValue, ReadableRow, QueryState) => AnyValue =
    (from, to) match {
      case (Some(f), Some(n)) => fullSlice(f, n)
      case (Some(f), None)    => fromSlice(f)
      case (None, Some(f))    => toSlice(f)
      case (None, None)       => (coll, _, _) => coll
    }

  private def fullSlice(
    from: Expression,
    to: Expression
  )(collectionValue: AnyValue, ctx: ReadableRow, state: QueryState) = {
    CypherFunctions.fullSlice(collectionValue, from(ctx, state), to(ctx, state))
  }

  private def fromSlice(from: Expression)(collectionValue: AnyValue, ctx: ReadableRow, state: QueryState) = {
    CypherFunctions.fromSlice(collectionValue, from(ctx, state))
  }

  private def toSlice(from: Expression)(collectionValue: AnyValue, ctx: ReadableRow, state: QueryState) = {
    CypherFunctions.toSlice(collectionValue, from(ctx, state))
  }

  override def apply(ctx: ReadableRow, state: QueryState): AnyValue =
    function(collection.apply(ctx, state), ctx, state)

  override def rewrite(f: Expression => Expression): Expression =
    f(ListSlice(collection.rewrite(f), from.map(_.rewrite(f)), to.map(_.rewrite(f))))

}
