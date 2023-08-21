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
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.storable.Values

case class SelectOrSemiApplySlottedPipe(
  lhs: Pipe,
  rhs: Pipe,
  predicate: Expression,
  negated: Boolean,
  slots: SlotConfiguration
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(lhs) with Pipe {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.filter {
      row =>
        (predicate.apply(row, state) eq Values.TRUE) || {
          val rhsState = state.withInitialContext(row)
          val innerResult = rhs.createResults(rhsState)
          val result = if (negated) !innerResult.hasNext else innerResult.hasNext
          innerResult.close()
          result
        }
    }.map {
      row: CypherRow =>
        val output = SlottedRow(slots)
        output.copyAllFrom(row)
        output
    }
  }
}
