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
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values

/*
This class is used for making the common <exp> IN <constant-expression> fast
 */
case class CachedIn(value: Expression, list: Expression, id: Id) extends Predicate with ListSupport {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    val listValue = list(ctx, state)
    if (listValue eq Values.NO_VALUE) {
      IsUnknown
    } else {
      val input = makeTraversable(listValue)
      if (input.isEmpty) {
        IsFalse
      } else {
        state.cachedIn.check(
          value(ctx, state),
          input,
          state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
        ) match {
          case IsNoValue()     => IsUnknown
          case b: BooleanValue => IsMatchResult(b.booleanValue())
          case v               => throw new IllegalStateException(s"$v is not a supported value for a predicate")
        }
      }
    }
  }

  override def children: Seq[AstNode[_]] = Seq(value, list)

  override def arguments: Seq[Expression] = Seq(list)

  override def rewrite(f: Expression => Expression): Expression =
    f(CachedIn(value.rewrite(f), list.rewrite(f), id))
}
