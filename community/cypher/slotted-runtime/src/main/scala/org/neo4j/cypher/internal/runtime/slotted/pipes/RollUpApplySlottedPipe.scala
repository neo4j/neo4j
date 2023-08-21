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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.virtual.VirtualValues

case class RollUpApplySlottedPipe(
  lhs: Pipe,
  rhs: Pipe,
  collectionRefSlotOffset: Int,
  identifierToCollect: (String, Expression),
  slots: SlotConfiguration
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(lhs) {

  private val getValueToCollectFunction = {
    val expression: Expression = identifierToCollect._2
    state: QueryState => (ctx: CypherRow) => expression(ctx, state)
  }

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.map {
      ctx =>
        val innerState = state.withInitialContext(ctx)
        val innerResults: ClosingIterator[CypherRow] = rhs.createResults(innerState)
        val collection = VirtualValues.list(innerResults.map(getValueToCollectFunction(state)).toArray: _*)
        ctx.setRefAt(collectionRefSlotOffset, collection)
        ctx
    }
  }
}
