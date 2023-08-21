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

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.kernel.impl.util.collection
import org.neo4j.kernel.impl.util.collection.ProbeTable
import org.neo4j.memory.Measurable

abstract class AbstractHashJoinPipe[Key <: Measurable](left: Pipe, right: Pipe) extends PipeWithSource(left) {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {

    if (!input.hasNext)
      return ClosingIterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return ClosingIterator.empty

    val table = buildProbeTable(input, state)
    state.query.resources.trace(table)

    // This will only happen if all the lhs-values evaluate to null, which is probably rare.
    // But, it's cheap to check and will save us from exhausting the rhs, so it's probably worth it
    if (table.isEmpty)
      return ClosingIterator.empty

    probeInput(rhsIterator, state, table)
  }

  def buildProbeTable(input: ClosingIterator[CypherRow], queryState: QueryState): collection.ProbeTable[Key, CypherRow]

  def probeInput(
    rhsInput: ClosingIterator[CypherRow],
    queryState: QueryState,
    probeTable: ProbeTable[Key, CypherRow]
  ): ClosingIterator[CypherRow]
}
