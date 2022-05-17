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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedPipeMapper.RowMapping
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.pipes.UnionSlottedPipe.mapRow
import org.neo4j.cypher.internal.util.attribution.Id

case class UnionSlottedPipe(
  lhs: Pipe,
  rhs: Pipe,
  slots: SlotConfiguration,
  lhsMapping: RowMapping,
  rhsMapping: RowMapping
)(val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    def left = lhs.createResults(state).map(mapRow(slots, lhsMapping, _, state))
    def right = rhs.createResults(state).map(mapRow(slots, rhsMapping, _, state))
    left ++ right

  }
}

object UnionSlottedPipe {

  def mapRow(slots: SlotConfiguration, mapping: RowMapping, input: CypherRow, state: QueryState): CypherRow = {
    val outgoing = SlottedRow(slots)
    mapping.mapRows(input, outgoing, state)
    outgoing
  }
}
