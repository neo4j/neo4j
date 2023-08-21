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

import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id

case class ForeachSlottedApplyPipe(lhs: Pipe, rhs: Pipe, innerVariableSlot: Slot, expression: Expression)(val id: Id =
  Id.INVALID_ID)
    extends PipeWithSource(lhs) with Pipe with ListSupport {

  // ===========================================================================
  // Compile-time initializations
  // ===========================================================================
  private val setVariableFun = makeSetValueInSlotFunctionFor(innerVariableSlot)

  // ===========================================================================
  // Runtime code
  // ===========================================================================
  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.map {
      outerContext =>
        val values = makeTraversable(expression(outerContext, state)).iterator()
        while (values.hasNext) {
          setVariableFun(outerContext, values.next()) // A slot for the variable has been allocated on the outer context
          val innerState = state.withInitialContext(outerContext)
          rhs.createResults(
            innerState
          ).size // exhaust the iterator, in case there's a merge read increasing cardinality inside the foreach
        }
        outerContext
    }
  }
}
