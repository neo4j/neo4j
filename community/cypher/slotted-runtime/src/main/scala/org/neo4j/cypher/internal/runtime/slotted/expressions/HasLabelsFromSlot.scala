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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

case class HasLabelFromSlot(offset: Int, resolvedLabelToken: Int) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    Some(state.query.isLabelSetOnNode(resolvedLabelToken, ctx.getLongAt(offset), state.cursors.nodeCursor))
  }

  override def containsIsNull: Boolean = false

  override def children: Seq[AstNode[_]] = Seq.empty[AstNode[_]]
}

case class HasLabelFromSlotLate(offset: Int, labelName: String) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    val maybeToken = state.query.getOptLabelId(labelName)
    val result =
      if (maybeToken.isEmpty)
        false
      else
        state.query.isLabelSetOnNode(maybeToken.get, ctx.getLongAt(offset), state.cursors.nodeCursor)

    Some(result)
  }

  override def containsIsNull: Boolean = false

  override def children: Seq[AstNode[_]] = Seq.empty[AstNode[_]]
}

case class HasAnyLabelFromSlot(offset: Int, resolvedLabelTokens: Array[Int]) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    Some(state.query.isAnyLabelSetOnNode(resolvedLabelTokens, ctx.getLongAt(offset), state.cursors.nodeCursor))
  }

  override def containsIsNull: Boolean = false

  override def children: Seq[AstNode[_]] = Seq.empty[AstNode[_]]
}

case class HasAnyLabelFromSlotLate(offset: Int, labelNames: Seq[String]) extends Predicate with SlottedExpression {

  override def isMatch(ctx: ReadableRow, state: QueryState): Option[Boolean] = {
    val tokens = labelNames.flatMap(state.query.getOptLabelId).toArray
    Some(state.query.isAnyLabelSetOnNode(tokens, ctx.getLongAt(offset), state.cursors.nodeCursor))
  }

  override def containsIsNull: Boolean = false

  override def children: Seq[AstNode[_]] = Seq.empty[AstNode[_]]
}
