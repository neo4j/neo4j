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

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsMatchResult
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

case class HasTypeFromSlot(offset: Int, resolvedTypeToken: Int) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    IsMatchResult(state.query.isTypeSetOnRelationship(
      resolvedTypeToken,
      ctx.getLongAt(offset),
      state.cursors.relationshipScanCursor
    ))
  }

  override def children: Seq[AstNode[_]] = Seq.empty[AstNode[_]]
}

case class HasTypeFromSlotLate(offset: Int, typeName: String) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): IsMatchResult = {
    val maybeToken = state.query.getOptRelTypeId(typeName)
    val result =
      if (maybeToken.isEmpty)
        false
      else
        state.query.isTypeSetOnRelationship(maybeToken.get, ctx.getLongAt(offset), state.cursors.relationshipScanCursor)

    IsMatchResult(result)
  }

  override def children: Seq[AstNode[_]] = Seq.empty[AstNode[_]]
}
